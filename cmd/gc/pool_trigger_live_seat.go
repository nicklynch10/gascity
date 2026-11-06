package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/pathutil"
	"github.com/gastownhall/gascity/internal/session"
)

// errPoolSeatRunsElsewhere marks a request whose work runs in a managed
// worktree that every live seat able to take it runs outside of, while no
// fresh session can start: the work is left for a session in its own
// directory (selectOrPlanPoolSessionBeadAvoiding, poolSeatRunsElsewhereError).
// In a full pool a seat retires so that one can start
// (poolSeatsElsewhere.freeSlot).
var errPoolSeatRunsElsewhere = errors.New("work left for a session in its own directory")

// errPoolWorktreeOccupied marks a request whose work runs in a managed
// worktree a live session still runs in. That worktree is that session's until
// it leaves: a second seat started there would put two live sessions in one
// checkout on one branch, each editing the other's work. The request waits for
// the occupant to go (selectOrPlanPoolSessionBeadAvoiding,
// poolWorktreeOccupiedError), and the reconciler retries it next tick.
var errPoolWorktreeOccupied = errors.New("worktree held by a live session")

// poolWorktreeOccupied is a managed worktree a request's work runs in and the
// live sessions that still run inside it, by session bead ID.
type poolWorktreeOccupied struct {
	// WorkDir is the work's verified managed worktree.
	WorkDir string
	// Seats are the open sessions whose row places them in WorkDir and whose
	// agent is alive, whether or not they are this pool's seats: the checkout
	// is theirs to edit whatever they are working.
	Seats []string
}

// poolWorktreeOccupiedError is errPoolWorktreeOccupied for one request: the
// worktree, the live sessions in it, and the trigger bead whose work waits.
type poolWorktreeOccupiedError struct {
	at     poolWorktreeOccupied
	workID string
}

func (e *poolWorktreeOccupiedError) Error() string {
	return fmt.Sprintf("%v: session %s runs in %s, where trigger bead %s's work runs; no second seat starts there",
		errPoolWorktreeOccupied, strings.Join(e.at.Seats, ", "), e.at.WorkDir, e.workID)
}

func (e *poolWorktreeOccupiedError) Unwrap() error { return errPoolWorktreeOccupied }

// poolSeatElsewhere is a live pool seat whose directory is not the one the
// controller places a request's work in.
type poolSeatElsewhere struct {
	// SeatDir is the directory the seat's row records, where its process
	// runs and which `gc hook --claim` reads as the seat's own
	// (hookClaimPoolSeatWorkDir).
	SeatDir string
	// WorkDir is the directory the controller places the request's work in:
	// its verified managed worktree, or the pool's own directory, routed by
	// pack and workspace, for work bound to none (poolWorkDirUnder).
	WorkDir string
	// Managed reports that WorkDir is the work's managed worktree. A seat
	// anywhere else always leaves such work (hookClaimWorktreeHeldElsewhere);
	// work bound to none it leaves only from inside a managed worktree
	// (hookClaimUnboundWorkOutsideSeat).
	Managed bool
}

// poolSeatPlacedElsewhere reports whether binding request to the pool seat
// info would re-point it at work the controller places in a directory other
// than the one its row records, and where both directories are. Whether the
// seat is alive is the caller's question (poolSeatAlive): a live seat is not
// restarted when its row is re-pointed, because the work dir is not a restart
// input (runtime.ConfigFingerprint) and the warm-bind nudge asks the running
// process to claim the work where it is (deliverWarmBindClaimNudge).
//
// Only a change of trigger re-points a seat; a resume of work assigned to it
// (poolRequestResumesAssignedWorkInfo) keeps computePoolTriggerBindingPatch's
// own rule, where a fresh-mode seat is cycled into the new directory. A
// request whose worktree evidence is unusable is left to the bind, which
// skips it. A row that is not a pool seat's or records no directory, and a
// seat already in the work's directory, are not placed elsewhere.
func poolSeatPlacedElsewhere(bp *agentBuildParams, cfgAgent *config.Agent, info session.Info, request SessionRequest) (poolSeatElsewhere, bool) {
	workID := strings.TrimSpace(request.WorkBeadID)
	if bp == nil || cfgAgent == nil || workID == "" || workID == strings.TrimSpace(info.TriggerBeadID) {
		return poolSeatElsewhere{}, false
	}
	if strings.TrimSpace(request.WorktreeError) != "" || poolRequestResumesAssignedWorkInfo(request, bp.assignedWorkBeads, info) {
		return poolSeatElsewhere{}, false
	}
	seatDir := hookClaimPoolSeatWorkDir(&info, cfgAgent)
	if seatDir == "" {
		return poolSeatElsewhere{}, false
	}
	at := poolSeatElsewhere{SeatDir: seatDir}
	if request.WorktreeSpec != nil {
		at.WorkDir, at.Managed = strings.TrimSpace(request.WorktreeSpec.Path), true
	} else if poolDir := strings.TrimSpace(hookClaimPoolSeatFor(bp.cityPath, bp.cityName, bp.rigs, &info, cfgAgent).PoolDir); poolDir != "" {
		at.WorkDir = poolWorkDirUnder(poolDir, request)
	}
	if at.WorkDir == "" || pathutil.SamePath(resolveWorkDirAgainstCity(bp.cityPath, at.SeatDir), at.WorkDir) {
		return poolSeatElsewhere{}, false
	}
	return at, true
}

// poolSeatAlive reports whether the agent of the pool seat info is alive,
// observed as the reconciler observes it (observeRuntimeProviderLiveness with
// the provider's process names), so a pane whose agent died is not alive and
// the reconciler restarts it where its row points. An observation that fails
// reads as alive: the seat may still run where its row says. No provider, or
// a row with no runtime name, is not alive.
func poolSeatAlive(bp *agentBuildParams, cfgAgent *config.Agent, info session.Info) bool {
	return poolSeatAliveAs(bp, poolSeatProcessNames(bp, cfgAgent), info)
}

// poolSeatAliveAs is poolSeatAlive with the provider's process names
// resolved (poolSeatProcessNames).
func poolSeatAliveAs(bp *agentBuildParams, processNames []string, info session.Info) bool {
	if bp == nil || bp.sp == nil {
		return false
	}
	name := strings.TrimSpace(info.SessionNameMetadata)
	if name == "" {
		return false
	}
	_, alive, err := observeRuntimeProviderLiveness(bp.sp, name, processNames)
	return alive || err != nil
}

// poolSeatProcessNames returns the process names of cfgAgent's provider, by
// which poolSeatAlive tells a live agent from a pane whose agent died; none
// when the provider does not resolve.
func poolSeatProcessNames(bp *agentBuildParams, cfgAgent *config.Agent) []string {
	if bp == nil || bp.lookPath == nil {
		return nil
	}
	resolved, err := config.ResolveProvider(cfgAgent, bp.workspace, bp.providers, bp.lookPath)
	if err != nil {
		return nil
	}
	return resolved.ProcessNames
}

// poolSeatRunsElsewhereError is errPoolSeatRunsElsewhere for one request:
// the live seat it passed over, where that seat runs and where the work runs,
// and why no fresh session can start (cause).
type poolSeatRunsElsewhereError struct {
	seat   session.Info
	at     poolSeatElsewhere
	workID string
	cause  error
}

func (e *poolSeatRunsElsewhereError) Error() string {
	return fmt.Sprintf("%v: session %s runs in %s, not in %s, where trigger bead %s's work runs, and no fresh session can start: %v",
		errPoolSeatRunsElsewhere, e.seat.ID, e.at.SeatDir, e.at.WorkDir, e.workID, e.cause)
}

func (e *poolSeatRunsElsewhereError) Unwrap() []error {
	return []error{errPoolSeatRunsElsewhere, e.cause}
}

// releasePoolPlan gives back what a refused fresh selection reserved: the
// plan's concrete slot, and its share of the pool create budget.
// planFreshPoolSessionBead claims the budget only after it has taken the slot,
// so a plan's existence means both are held: the caller gives both back or
// leaks both, and the pass is not budgeted anyway (admission takes plans in
// fair-share order, so a held share would only starve the next tick's creates
// for no gain). A selection that reused a row carries no plan and holds no
// budget share; the slot it claimed (claimDesiredPoolSlotInfo) is the row's
// own, and stays claimed so no fresh plan is named into it while the row
// exists.
func (bp *agentBuildParams) releasePoolPlan(plan *poolSessionCreatePlan, usedSlots map[int]bool) {
	if bp == nil || plan == nil {
		return
	}
	delete(usedSlots, plan.slot)
	bp.releasePoolSessionCreate()
}

// poolSessionRunsIn reports whether the open session info's row places it in
// dir: the directory its process runs in, resolved against cityPath as the
// controller resolves a recorded work dir. An empty recorded directory names
// none.
func poolSessionRunsIn(cityPath string, info session.Info, dir string) bool {
	recorded := preparedStartSessionWorkDir(info)
	if recorded == "" || dir == "" {
		return false
	}
	return pathutil.SamePath(resolveWorkDirAgainstCity(cityPath, recorded), dir)
}

// poolOccupantInfos is the rows a worktree occupancy check reads: the build's
// cross-store occupancy census, which holds rows the pool's own snapshot does
// not, such as a session another store's leg reports, together with the
// build's own session snapshot, which holds the seats this build has placed;
// the census alone for a build with no snapshot.
//
// The fence is built over a pass's or a pool's params, never over the v2
// realize view: that view's realize memo narrows freshPoolOccupancyInfos to
// one agent's slots, and another pool's seat in the same checkout is exactly
// what this must see (the "occupant of another pool" case of
// TestAllocator_LiveSeatInTheWorksWorktreeRefusesASecondSeat).
func poolOccupantInfos(bp *agentBuildParams) []session.Info {
	if bp == nil {
		return nil
	}
	if bp.sessionBeads == nil {
		return bp.sessionOccupancyInfos
	}
	return freshPoolOccupancyInfos(bp)
}

// poolWorktreeOccupiedBy returns the open sessions that run in the managed
// worktree dir: every row the build knows whose recorded directory is dir and
// which live reports alive. A row that is not a pool seat counts too — a
// checkout is held whatever its occupant is working, and by whichever pool it
// belongs to. live decides each row's liveness, and it must fail closed: a row
// it cannot observe counts as alive.
func poolWorktreeOccupiedBy(bp *agentBuildParams, dir string, live func(session.Info) bool) []string {
	if bp == nil || dir == "" || live == nil {
		return nil
	}
	var seats []string
	for _, info := range poolOccupantInfos(bp) {
		if !poolSessionRunsIn(bp.cityPath, info, dir) {
			continue
		}
		if !live(info) {
			continue
		}
		seats = append(seats, info.ID)
	}
	return seats
}

// poolSeatLiveByProbe is poolWorktreeOccupiedBy's default live: the row's
// agent is alive as the reconciler observes it, with the process names of the
// agent its row's template names (poolSeatProcessNames), so a pane whose agent
// died does not hold a checkout. It fails closed, as the fence requires: an
// observation that fails (poolSeatAliveAs), a row with no runtime name to
// probe, and a build with no provider to probe it with all read alive, since
// the checkout may still be edited.
func poolSeatLiveByProbe(bp *agentBuildParams, cfgAgent *config.Agent) func(session.Info) bool {
	return func(info session.Info) bool {
		// poolSeatAlive answers "may this seat be re-pointed", where an
		// unobservable seat reads not-alive. The fence asks the opposite
		// question about the same observation, so the cases it cannot read
		// are held rather than ignored.
		if bp == nil || bp.sp == nil || strings.TrimSpace(info.SessionNameMetadata) == "" {
			return true
		}
		return poolSeatAlive(bp, poolSeatConfigAgent(bp, cfgAgent, info), info)
	}
}

// poolSeatConfigAgent is the agent a row's liveness is observed with: the
// configured agent its template names, else fallback. The row may be another
// pool's seat, or the agent its own row names may be gone from the config.
func poolSeatConfigAgent(bp *agentBuildParams, fallback *config.Agent, info session.Info) *config.Agent {
	template := resolvedSessionTemplateInfo(info, reuseTemplateConfig(bp))
	if template == "" {
		return fallback
	}
	if cfg := findAgentByTemplate(reuseTemplateConfig(bp), template); cfg != nil {
		return cfg
	}
	return fallback
}

// poolSeatsElsewhere selects and plans, for one legacy realization of a pool,
// around the live seats a request would re-point at work placed elsewhere
// (poolSeatPlacedElsewhere and poolSeatAlive), observing each seat at most
// once and resolving the provider's process names once. It stays out of the
// v2 allocator's reach: selectOrPlanPoolSessionBeadAvoiding, which v2 shares,
// must stay pure, so the seats are observed here and passed to it as data.
type poolSeatsElsewhere struct {
	bp           *agentBuildParams
	cfgAgent     *config.Agent
	template     string
	alive        map[string]bool
	processNames []string
	resolved     bool
	// fence refuses a selection that would put a session in a managed
	// worktree a live one still runs in. It shares this selection's liveness
	// read, so a seat is observed once however it is asked about.
	fence *poolWorktreeFence
}

func newPoolSeatsElsewhere(bp *agentBuildParams, cfgAgent *config.Agent, template string) *poolSeatsElsewhere {
	s := &poolSeatsElsewhere{bp: bp, cfgAgent: cfgAgent, template: template, alive: map[string]bool{}}
	s.fence = &poolWorktreeFence{bp: bp, cfgAgent: cfgAgent, live: s.isAlive}
	return s
}

// selectOrPlan is selectOrPlanPoolSessionBeadAvoiding for request, passing
// over the live seats elsewhere it could choose (forRequest). When that
// leaves the request waiting at a full pool, it frees a slot (freeSlot) and
// adds the seats retired to the error.
func (s *poolSeatsElsewhere) selectOrPlan(preferred *session.Info, request SessionRequest, decisionTime time.Time, used map[string]bool, usedSlots map[int]bool) (session.Info, int, *poolSessionCreatePlan, error) {
	info, slot, plan, err := selectOrPlanPoolSessionBeadAvoiding(s.bp, s.cfgAgent, s.template, preferred, request, decisionTime, used, usedSlots, s.forRequest(request, used))
	if held := s.fence.occupiedByAnother(info, plan, request); held != nil {
		// The selection would put a session in a worktree a live one still
		// runs in. Nothing about the pool can free that: the occupant is not
		// this pool's to retire, and it leaves when its own work ends. The
		// plan's reservation goes back so a later request, or the next tick,
		// can use the slot.
		s.bp.releasePoolPlan(plan, usedSlots)
		return session.Info{}, 0, nil, held
	}
	if retired := s.freeSlot(request, err, used); len(retired) > 0 {
		err = fmt.Errorf("%w; freeing a slot: session %s leaves the desired set", err, strings.Join(retired, ", "))
	}
	return info, slot, plan, err
}

// poolWorktreeFence holds one selection's view of which managed worktrees a
// live session still runs in. It is shared by both reconcilers: legacy builds
// it per pool (poolSeatsElsewhere), v2 per agent's realization, over the
// liveness its census pass already read, so neither observes a seat twice.
type poolWorktreeFence struct {
	bp       *agentBuildParams
	cfgAgent *config.Agent
	// live decides an occupant's liveness (poolWorktreeOccupiedBy). It must
	// fail closed: a row it cannot observe counts as alive. Nil reads the
	// runtime (poolSeatLiveByProbe).
	live func(session.Info) bool
	// occupied caches each work dir's occupancy by path, so the requests for
	// one worktree probe its occupants once.
	occupied map[string]poolWorktreeOccupied
}

// occupiedByAnother reports the worktree a selection would put a session into
// that a live session still runs in, or nil. A selection that keeps a seat in
// the worktree it runs in is that worktree's own seat, not another: there is
// one live session there either way. Every other selection — a fresh seat
// planned in the work's worktree, or a seat moved into it — is refused while an
// occupant runs there, whatever pool it belongs to. Work bound to no managed
// worktree is never affected: it is placed in the pool's own directory, which
// no worktree fence owns.
func (f *poolWorktreeFence) occupiedByAnother(info session.Info, plan *poolSessionCreatePlan, request SessionRequest) *poolWorktreeOccupiedError {
	if f == nil || f.bp == nil || request.WorktreeSpec == nil {
		return nil
	}
	workDir := strings.TrimSpace(request.WorktreeSpec.Path)
	if workDir == "" {
		return nil
	}
	live := f.live
	if live == nil {
		live = poolSeatLiveByProbe(f.bp, f.cfgAgent)
	}
	// A live selected row that already runs in the worktree keeps its seat:
	// the selection places no new session there, so there is nothing to
	// refuse, and whatever else runs beside it is not this request's doing.
	if plan == nil && info.ID != "" && poolSessionRunsIn(f.bp.cityPath, info, workDir) && live(info) {
		return nil
	}
	all, cached := f.occupied[workDir]
	if !cached {
		if f.occupied == nil {
			f.occupied = map[string]poolWorktreeOccupied{}
		}
		all = poolWorktreeOccupied{WorkDir: workDir, Seats: poolWorktreeOccupiedBy(f.bp, workDir, live)}
		f.occupied[workDir] = all
	}
	// The cache is the worktree's occupants, whoever asked: the selected row
	// itself is not one of the rows it must be placed beside.
	at := poolWorktreeOccupied{WorkDir: all.WorkDir}
	for _, id := range all.Seats {
		if id != info.ID {
			at.Seats = append(at.Seats, id)
		}
	}
	if len(at.Seats) == 0 {
		return nil
	}
	return &poolWorktreeOccupiedError{at: at, workID: request.WorkBeadID}
}

// forRequest returns, by session ID, the live seats a selection for request
// could choose that run outside the directory its work is placed in. A
// selection chooses only the seat the request names and the pool's reusable
// seats (reusablePoolSessionInfo), so no other seat is observed, such as a
// busy one no request for other work can reuse.
func (s *poolSeatsElsewhere) forRequest(request SessionRequest, used map[string]bool) map[string]poolSeatElsewhere {
	if s == nil || s.bp == nil || s.bp.sessionBeads == nil || strings.TrimSpace(request.WorkBeadID) == "" {
		return nil
	}
	var seats map[string]poolSeatElsewhere
	for _, info := range s.bp.sessionBeads.OpenInfos() {
		if info.ID != request.SessionBeadID && !reusablePoolSessionInfo(s.bp, s.cfgAgent, s.template, info, used) {
			continue
		}
		at, placed := poolSeatPlacedElsewhere(s.bp, s.cfgAgent, info, request)
		if !placed || !s.isAlive(info) {
			continue
		}
		if seats == nil {
			seats = map[string]poolSeatElsewhere{}
		}
		seats[info.ID] = at
	}
	return seats
}

func (s *poolSeatsElsewhere) isAlive(info session.Info) bool {
	alive, observed := s.alive[info.ID]
	if !observed {
		if !s.resolved {
			s.processNames, s.resolved = poolSeatProcessNames(s.bp, s.cfgAgent), true
		}
		alive = poolSeatAliveAs(s.bp, s.processNames, info)
		s.alive[info.ID] = alive
	}
	return alive
}

// freeSlot frees a slot of a full pool (err is errPoolSessionNoFreeSlot) for
// request's work, which runs in a managed worktree, so that work does not
// wait on seats that never leave, such as those a min_active_sessions floor
// keeps. Each seat it retires is left out of this build's desired set
// (agentBuildParams.retirePoolSeat), and used keeps the pool's later
// requests, the floor fill's included, off it. The reconciler drains a live
// retired seat as orphaned, a drain the floor does not veto, and closes the
// row of one that is not running. A seat already leaving the pool
// (poolSeatLeaving) frees its slot by itself: such seats retire, so that
// their rows close, and no other seat does. Otherwise the live seat the
// request passed over (poolSeatRunsElsewhereError) retires unless it holds
// assigned work. It returns the IDs of the seats retired.
func (s *poolSeatsElsewhere) freeSlot(request SessionRequest, err error, used map[string]bool) []string {
	if s == nil || s.bp == nil || s.bp.sessionBeads == nil || request.WorktreeSpec == nil || !errors.Is(err, errPoolSessionNoFreeSlot) {
		return nil
	}
	var retired []string
	for _, info := range s.bp.sessionBeads.OpenInfos() {
		if !used[info.ID] && s.ofPool(info) && poolSeatLeaving(s.bp, info) {
			retired = append(retired, s.retire(info, used))
		}
	}
	var passed *poolSeatRunsElsewhereError
	if len(retired) == 0 && errors.As(err, &passed) && !sessionBeadHasAssignedWorkInfo(s.bp.assignedWorkBeads, passed.seat, reuseTemplateConfig(s.bp)) {
		retired = append(retired, s.retire(passed.seat, used))
	}
	return retired
}

func (s *poolSeatsElsewhere) retire(info session.Info, used map[string]bool) string {
	if used != nil {
		used[info.ID] = true
	}
	s.bp.retirePoolSeat(info.ID)
	return info.ID
}

// ofPool reports whether info is a pool-managed seat of the pool.
func (s *poolSeatsElsewhere) ofPool(info session.Info) bool {
	return isPoolManagedSessionInfo(info) && !isNamedSessionInfo(info) && !isManualSessionInfoForAgent(info, s.cfgAgent) &&
		resolvedSessionTemplateInfo(info, reuseTemplateConfig(s.bp)) == s.template
}

// poolSeatLeaving reports whether the pool seat info is leaving the pool, its
// slot about to free with no other seat retiring: its runtime is draining, or
// it no longer runs and nothing keeps its slot. That is a seat drained or
// asleep with no hold (isPoolSessionSlotFreeableInfo), or asleep after the
// reconciler drained it as orphaned (CompleteDrainPatch records the drain's
// reason as the sleep reason). A seat an operator suspended, or one holding
// assigned work, is not leaving.
func poolSeatLeaving(bp *agentBuildParams, info session.Info) bool {
	if session.DispositionOfInfo(info).KeepsSlot() || sessionBeadHasAssignedWorkInfo(bp.assignedWorkBeads, info, reuseTemplateConfig(bp)) {
		return false
	}
	switch session.State(strings.TrimSpace(info.MetadataState)) {
	case session.StateDraining:
		return true
	case session.StateAsleep:
		if strings.TrimSpace(info.SleepReason) == drainOrphaned {
			return true
		}
	}
	return isPoolSessionSlotFreeableInfo(info, bp.now())
}

// retirePoolSeat records that legacy realization retired the pool seat id
// this build (poolSeatsElsewhere.freeSlot).
func (bp *agentBuildParams) retirePoolSeat(id string) {
	if bp.retiredPoolSeats == nil {
		bp.retiredPoolSeats = map[string]bool{}
	}
	bp.retiredPoolSeats[id] = true
}

// poolTriggerBindWorkDir is the work dir bindPoolSessionTriggerBead binds the
// pool seat info to for request: the request's verified work dir
// (verifiedPoolTriggerWorkDir), except for a live seat it would re-point at
// work placed in another directory, which keeps the directory its row records.
// Legacy realization hands such a seat work bound to no managed worktree only
// when no fresh session can start (selectOrPlanPoolSessionBeadAvoiding), so
// the seat's claim decides whether it runs that work there: it leaves it from
// inside a managed worktree. The request's evidence is still verified first,
// so unusable evidence skips the item as before. A live seat whose name was
// refused (SessionRequest.NameRefused) gets no work dir and is bound unnamed
// where it runs; one that is not alive moves to the pool's own directory
// (poolSeatBindingPatch).
func poolTriggerBindWorkDir(bp *agentBuildParams, cfgAgent *config.Agent, qualifiedName string, info session.Info, request SessionRequest) (string, error) {
	workDir, err := verifiedPoolTriggerWorkDir(bp, cfgAgent, qualifiedName, request)
	if err != nil || workDir == "" {
		return workDir, err
	}
	if request.NameRefused && poolSeatAlive(bp, cfgAgent, info) {
		return "", nil
	}
	at, placed := poolSeatPlacedElsewhere(bp, cfgAgent, info, request)
	if !placed || !poolSeatAlive(bp, cfgAgent, info) {
		return workDir, nil
	}
	if bp.stderr != nil {
		fmt.Fprintf(bp.stderr, "buildDesiredState: pool %q session %s keeps work dir %s, where it runs, for trigger bead %s, whose work runs in %s; its claim decides whether it runs that work there\n", qualifiedName, info.ID, at.SeatDir, request.WorkBeadID, workDir) //nolint:errcheck // best-effort stderr
	}
	return at.SeatDir, nil
}

// sameTriggerWorkDirStale reports whether binding the pool seat info to
// request must move its row off the work dir it records: the request is for
// the trigger the row already records (computePoolTriggerBindingPatch keeps
// the recorded dir for that), its work carries a verified managed worktree
// (request.WorktreeSpec), and the recorded dir, resolved against cityPath, is
// not workDir, that worktree. The work gained or changed its evidence after
// the seat was bound, or a live seat re-pointed at it kept the dir it ran in.
// The comparison is lexical, as the v2 decision is pure: legacy passes the
// path its verify reported, v2 the spec's path (its plan-only work dir is
// empty), which is the path the PreWake's verify reports.
// A seat started in the recorded dir would leave its own trigger to a session
// in the trigger's worktree and drain. Callers ask this only for a seat that
// can start: a running seat keeps the dir its process runs in.
func sameTriggerWorkDirStale(cityPath string, info session.Info, request SessionRequest, workDir string) bool {
	workID := strings.TrimSpace(request.WorkBeadID)
	if workID == "" || workDir == "" || request.WorktreeSpec == nil || workID != strings.TrimSpace(info.TriggerBeadID) {
		return false
	}
	recorded := preparedStartSessionWorkDir(info)
	return recorded != "" && filepath.Clean(resolveWorkDirAgainstCity(cityPath, recorded)) != filepath.Clean(workDir)
}
