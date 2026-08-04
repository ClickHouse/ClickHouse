#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <thread>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace ProfileEvents
{
    extern const Event CASIdentityLost;
    extern const Event CASDataRootVanished;
}

namespace DB::Cas
{

namespace
{
/// Wall-clock seconds since epoch — the `since` timestamp the lifecycle snapshot reports (spec §7). A
/// wall clock, deliberately unlike the fence's `CLOCK_BOOTTIME`: this is an operator-facing DateTime, not
/// an interval measured across a possible VM suspend.
int64_t wallClockNowSeconds()
{
    return static_cast<int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}
}

CasMountRuntime::CasMountRuntime(
    BackendPtr backend_ptr_,
    const Layout & layout_,
    MountConfig config_,
    String server_root_id_,
    const CasEventSink & event_sink_,
    CasRequestBudget cas_request_budget_,
    std::function<bool()> remount_attempt_)
    : backend_ptr(std::move(backend_ptr_))
    , layout(layout_)
    , config(std::move(config_))
    , server_root_id(std::move(server_root_id_))
    , event_sink(event_sink_)
    , cas_request_budget(cas_request_budget_)
    , remount_attempt(std::move(remount_attempt_))
{
}

uint64_t CasMountRuntime::bootMs()
{
    struct timespec ts{};
    clock_gettime(CLOCK_BOOTTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000 + static_cast<uint64_t>(ts.tv_nsec) / 1000000;
}

uint64_t CasMountRuntime::bootMsNow() const
{
    return config.boot_ms_fn ? config.boot_ms_fn() : bootMs();
}

void CasMountRuntime::waitSleep(uint64_t ms) const
{
    if (config.wait_sleep_fn)
        config.wait_sleep_fn(ms);
    else
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

bool CasMountRuntime::mayMutate() const
{
    return !mount_fence.lost.load(std::memory_order_acquire)
        && bootMsNow() < mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
}

void CasMountRuntime::tripMountLost()
{
    mount_fence.lost.store(true, std::memory_order_release);
    /// A durable-effect caller admitted under the incarnation this trip just ended must never conclude
    /// the fence is fine again just because a LATER `armMountFence` happens to re-arm it (rev.7 [C2]).
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
    /// The lease-loss event is exactly the `Live -> TransientNotLive` transition of the §1 state model.
    /// Idempotent and terminal-safe (a compare-exchange from `Live` only).
    noteLeaseLost();
}

void CasMountRuntime::checkFenceOrThrow(uint64_t admitted_generation) const
{
    /// [D5]: tell only what is known here. A tripped fence (or a bumped generation) means this node no
    /// longer holds the mount incarnation the caller was admitted under -- but this same guard trips for a
    /// transient lease blip AND for a deliberate terminal decommission (FORGET) or a lost identity, and this
    /// code cannot tell them apart. So the CONDITION must NOT promise recovery ("temporarily unreachable"
    /// would misdiagnose the terminal case); it names both possibilities and points at the authoritative
    /// lifecycle. The CLASS is the write plane's uniform transient one (its 32 sibling write-transient sites
    /// already mint it): under genuine ambiguity the refusal must be retried, never consumed as damage.
    if (!mayMutate() || fenceGeneration() != admitted_generation)
        throwCasTransientUnavailable(
            fmt::format("content-addressed pool '{}'", server_root_id),
            "mount fence tripped: the durable write is refused because this node no longer holds the mount "
            "incarnation it was admitted under -- either a lease loss the disk auto-recovers from, or a "
            "FORGET decommission / lost identity that does NOT recover; consult "
            "system.cas_mounts for the disk's lifecycle before retrying");
}

bool CasMountRuntime::refAppendFenceOk() const
{
    /// `mayMutate` checks the latch and deadline. The additional budget check prevents starting a
    /// controlled request that cannot plausibly finish, including its safety margin, before expiry.
    if (mount_fence.lost.load(std::memory_order_acquire))
        return false;
    const uint64_t now = bootMsNow();
    const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
    if (now >= deadline)
        return false;
    const uint64_t margin = cas_request_budget.attempt_timeout_ms + cas_request_budget.lease_safety_margin_ms;
    return margin < deadline - now;
}

void CasMountRuntime::setMountDeadline(uint64_t deadline_boot_ms)
{
    mount_fence.deadline_boot_ms.store(deadline_boot_ms, std::memory_order_release);
}

void CasMountRuntime::armMountFence(UInt128 server_uuid, uint64_t writer_epoch, uint64_t deadline_boot_ms)
{
    mount_fence.server_uuid = server_uuid;
    mount_fence.writer_epoch = writer_epoch;
    mount_fence.deadline_boot_ms.store(deadline_boot_ms, std::memory_order_release);
    /// A fresh lease incarnation is a fresh generation too: a durable-effect caller admitted under the
    /// PRIOR incarnation must re-check and abort rather than ride this re-arm through (rev.7 [C2]).
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
    if (arm_mount_fence_interposition_hook_for_test)
        arm_mount_fence_interposition_hook_for_test();
    /// Open the gate LAST. A caller that observes `lost == false` with acquire semantics must also see
    /// the fresh generation; publishing the latch first exposes one admission window in which the dead
    /// generation looks live again.
    mount_fence.lost.store(false, std::memory_order_release);
}

uint64_t CasMountRuntime::minActive()
{
    std::lock_guard lk(builds_mutex);
    return active_build_seqs.empty() ? next_build_seq : *active_build_seqs.begin();
}

uint64_t CasMountRuntime::peekNextBuildSeq()
{
    std::lock_guard lk(builds_mutex);
    return next_build_seq;
}

void CasMountRuntime::renewWatermarkOnce()
{
    /// A read-only runtime has no heartbeat to renew. Report that misuse instead of fabricating a keeper
    /// or silently treating the call as successful.
    if (!mount_keeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS heartbeat: renewWatermarkOnce on a read-only Pool");
    mount_keeper->renewOnce();
}

uint64_t CasMountRuntime::allocateBuildSeq()
{
    std::lock_guard lk(builds_mutex);
    const uint64_t s = next_build_seq++;
    active_build_seqs.insert(s);
    return s;
}

void CasMountRuntime::registerInflightBuild(uint64_t seq, const PartWriteTxnPtr & build)
{
    /// The caller owns the build's shared pointer. Keep only a weak reference here so the registry does
    /// not extend the build lifetime; publication, abandonment, or destruction removes the entry.
    std::lock_guard lk(builds_mutex);
    inflight_builds[seq] = build;
}

void CasMountRuntime::retireBuildSeq(uint64_t seq)
{
    std::lock_guard lk(builds_mutex);
    active_build_seqs.erase(seq);
    inflight_builds.erase(seq);
}

void CasMountRuntime::cancelInflightBuildsForNamespace(const RootNamespace & ns)
{
    /// The removal callback is invoked only after the namespace-removal transaction is durable. Keep
    /// cancellation outside `builds_mutex`; `cancelForNamespaceRemoval` changes the build's atomic
    /// cancellation state and does not require the registry lock.
    std::vector<PartWriteTxnPtr> builds_to_check;
    {
        std::lock_guard lk(builds_mutex);
        for (const auto & entry : inflight_builds)
            if (auto build = entry.second.lock())
                builds_to_check.push_back(std::move(build));
    }
    for (const auto & build : builds_to_check)
        build->cancelForNamespaceRemoval(ns);
}

void CasMountRuntime::mintRandomProcessEpoch()
{
    /// Mint a nonzero equality-only identity. Keep it away from the zero/unarmed and UINT64_MAX/retired
    /// sentinels; 52 random bits are sufficient for the expected collision risk of this token.
    constexpr uint64_t EPOCH_MASK = (1ULL << 52) - 1;
    process_epoch.store(
        (thread_local_rng() ^ (static_cast<uint64_t>(thread_local_rng()) << 32)) & EPOCH_MASK,
        std::memory_order_relaxed);
    if (process_epoch.load(std::memory_order_relaxed) == 0)
        process_epoch.store(1, std::memory_order_relaxed);
}

void CasMountRuntime::setProcessEpoch(uint64_t v, std::memory_order order)
{
    process_epoch.store(v, order);
}

void CasMountRuntime::setLiveWriterEpoch(uint64_t v)
{
    live_writer_epoch.store(v, std::memory_order_release);
}

void CasMountRuntime::installKeeper(UInt128 our_uuid, uint64_t writer_epoch, const std::function<uint64_t()> & now_ms)
{
    /// The mount object already contains this runtime's live `(uuid, epoch)` body. Construct the keeper
    /// to adopt that exact slot rather than triggering its double-start guard. The keeper reads the
    /// build-watermark floor through `minActive` while preparing each renewal.
    const uint64_t ttl_ms = static_cast<uint64_t>(config.mount_lease_ttl_ms.count());
    mount_keeper = std::make_unique<MountLeaseKeeper>(
        backend_ptr, layout, server_root_id, our_uuid, writer_epoch,
        config.mount_lease_ttl_ms, now_ms,
        [this] { return minActive(); },
        [this](CasEvent e) { emitEvent(std::move(e)); },
        std::chrono::milliseconds(cas_request_budget.lease_safety_margin_ms),
        [this] { return bootMsNow(); });
    /// Install the fence callbacks before any background renewal can run: successful renewals extend the
    /// local BOOTTIME deadline, while a superseded or foreign renewal latches the fence and starts recovery.
    mount_keeper->setFenceCallbacks(
        [this, ttl_ms](uint64_t attempt_boot_ms) { setMountDeadline(attempt_boot_ms + ttl_ms); },
        [this]
        {
            tripMountLost();
            /// Recover as a fresh incarnation; a fenced `(uuid, writer_epoch)` pair is never resurrected.
            scheduleRemount();
        });
}

void CasMountRuntime::keeperStart()
{
    mount_keeper->start();
}

void CasMountRuntime::keeperRenewOnce()
{
    mount_keeper->renewOnce();
}

void CasMountRuntime::keeperReset()
{
    mount_keeper.reset();
}

void CasMountRuntime::keeperStartBackground(std::chrono::milliseconds period)
{
    mount_keeper->startBackground(period);
}

void CasMountRuntime::keeperStopBackground()
{
    mount_keeper->stopBackground();
}

bool CasMountRuntime::isVanished() const
{
    const PoolLifecycle s = lifecycle();
    return s == PoolLifecycle::VanishedReplaced
        || s == PoolLifecycle::VanishedForgotten;
}

void CasMountRuntime::setLifecycleForTest(PoolLifecycle lc)
{
    /// Direct store, no precondition — the test harness pins an exact cell of the class × state table.
    /// A `Vanished*` value also latches `vanished_intent` so the forced terminal state matches what a
    /// natural `enterVanished` would leave behind (its truth semantics never depend on how it was reached).
    /// Stamp `since` to match a naturally-reached state (release-store before the state store below, so a
    /// snapshot reader that acquire-observes the forced state also observes the timestamp): 0 for `Live`,
    /// now for every non-`Live` value. Keeps the forced cell of the class × state table indistinguishable
    /// from a real transition for the introspection snapshot.
    lifecycle_since_wall_s.store(lc == PoolLifecycle::Live ? 0 : wallClockNowSeconds(), std::memory_order_release);
    pool_lifecycle.store(lc, std::memory_order_release);
    if (lc == PoolLifecycle::VanishedReplaced
        || lc == PoolLifecycle::VanishedForgotten)
    {
        vanished_intent.store(true, std::memory_order_release);
        /// Keep the terminal-state guard consistent with the forced state, so a later `enterVanished`
        /// (unusual, but not forbidden) is a clean no-op rather than re-storing / re-logging.
        terminal_state_published.store(true, std::memory_order_release);
    }
}

void CasMountRuntime::noteLeaseLost()
{
    /// `Live -> TransientNotLive`, and nothing else. A compare-exchange FROM `Live` leaves every other
    /// state untouched, so a terminal state is never downgraded and a repeated call is a no-op. This is
    /// the only transition the keeper thread performs, and it needs no lock because of that discipline.
    PoolLifecycle expected = PoolLifecycle::Live;
    if (pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::TransientNotLive, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        /// The `since` the lifecycle snapshot reports for `not_live` — the wall-clock instant this became
        /// non-`Live`. Only the winning transition writes it (the guard above), so it is not re-stamped.
        lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);
    }
}

void CasMountRuntime::noteRemounted()
{
    /// `TransientNotLive -> Live` on a successful reclaim. A compare-exchange FROM `TransientNotLive`
    /// never revives `IdentityLost` or a `Vanished` state ([D3]) and is a no-op if already `Live`.
    PoolLifecycle expected = PoolLifecycle::TransientNotLive;
    if (pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::Live, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        /// Back to `Live`: the lifecycle snapshot reports no `since` (0) for a live pool.
        lifecycle_since_wall_s.store(0, std::memory_order_release);
    }
}

void CasMountRuntime::enterIdentityLost()
{
    /// `TransientNotLive -> IdentityLost`, one way. The compare-exchange FROM `TransientNotLive` gives
    /// the brief's "from TransientNotLive only" precondition, idempotency (a second call finds the state
    /// already `IdentityLost` and its exchange fails), and safety against a concurrent keeper
    /// `noteLeaseLost` (which only ever moves `Live -> TransientNotLive`, never away from it). It does NOT
    /// set `vanished_intent` (that latch is reserved for the `Vanished*` idempotency/FORGET protocol);
    /// rev.8 makes `IdentityLost` a fail-loud TERMINAL state through `remountTerminal()`, which folds it
    /// into the observer-exit boundary alongside `vanished_intent`, so the remount/GC observer threads
    /// self-exit rather than demote.
    /// `since` for the `identity_lost` snapshot row — the wall-clock instant the observer proved the
    /// sentinels gone. Stamped (release) BEFORE the CAS that publishes `IdentityLost`, so a reader that
    /// acquire-observes `IdentityLost` is guaranteed to observe this timestamp too (the winning CAS's
    /// release carries this prior store) — the same before-publish ordering `enterVanished` uses. Safe to
    /// stamp before the CAS here, unlike the lock-free `noteLeaseLost`: this runs only from
    /// `TransientNotLive` under `Pool::remount_mutex` (a `Vanished` pool bailed at the caller's
    /// `isVanished()` gate and the caller guards `!= IdentityLost`), so the CAS wins deterministically and
    /// the stamp can never land on a state we did not transition.
    lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);

    PoolLifecycle expected = PoolLifecycle::TransientNotLive;
    if (!pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::IdentityLost, std::memory_order_acq_rel, std::memory_order_acquire))
        return;

    ProfileEvents::increment(ProfileEvents::CASIdentityLost);
    LOG_WARNING(getLogger("CasPool"),
        "Content-addressed pool '{}' entered IdentityLost: the pool sentinels (_pool_meta and the owner "
        "anchor) are authoritatively absent (both KeyAbsent). This is a fail-loud TERMINAL state: "
        "store-class access now fails loud and this pool's remount + GC threads self-exit. "
        "Recover by restart or SYSTEM CAS FORGET — a matching-sentinel restore does NOT "
        "auto-revive this disk.",
        server_root_id);
}

void CasMountRuntime::enterVanished(PoolLifecycle which, const String & reason)
{
    /// Validate the target BEFORE mutating any state — `enterVanished` takes only the two `Vanished*`
    /// values (`VanishedReplaced`/`VanishedForgotten`); fail loud on a call-site bug rather than store a
    /// non-terminal value or mislabel it.
    const char * label = nullptr;
    switch (which)
    {
        case PoolLifecycle::VanishedReplaced:  label = "replaced"; break;
        case PoolLifecycle::VanishedForgotten: label = "forgotten"; break;
        case PoolLifecycle::Live:
        case PoolLifecycle::TransientNotLive:
        case PoolLifecycle::IdentityLost:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CasMountRuntime::enterVanished called with a non-terminal lifecycle value");
    }

    /// Publish the terminal-intent latch (spec §3). For a natural transition this is the FIRST publish; for
    /// FORGET, `publishVanishedIntent` already set it at step 1. Either way it is published before the state
    /// store below.
    vanished_intent.store(true, std::memory_order_release);

    /// Idempotency guard for the STATE transition, keyed on a dedicated latch rather than
    /// `vanished_intent` (which FORGET publishes early): the FIRST winner stores the state, records the
    /// reason, and logs; a later call returns here without re-storing or re-logging.
    if (terminal_state_published.exchange(true, std::memory_order_acq_rel))
        return;

    /// Record the reason BEFORE the state's release-store, so a reader that acquire-observes the terminal
    /// state (e.g. `Pool::throwIfLifecycleTerminal`) also observes this string. Written exactly once.
    vanished_reason = reason;

    /// The `since` the lifecycle snapshot reports for the `vanished` row — the wall-clock instant of the
    /// terminal transition. Written before the `pool_lifecycle` release-store below, same as the reason,
    /// so an acquire-observer of the terminal state also observes it.
    lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);

    /// An unconditional store is safe now: the guard above serializes terminal transitions, and no
    /// non-terminal transition can move a `Vanished` state (their compare-exchanges are keyed on
    /// `Live`/`TransientNotLive`), so this value is absorbing.
    pool_lifecycle.store(which, std::memory_order_release);

    ProfileEvents::increment(ProfileEvents::CASDataRootVanished);
    LOG_WARNING(getLogger("CasPool"),
        "Content-addressed pool '{}' entered Vanished({}): {}. The disk stays registered but store-class "
        "access now fails loud with a typed error (truth); restart re-registers the name.",
        server_root_id, label, reason);
}

void CasMountRuntime::publishVanishedIntent()
{
    /// spec §5 step 1: publish the terminal-intent latch WITHOUT settling the state. The keeper callback
    /// (`scheduleRemount`) and the remount loop both consult `vanished_intent` at their step boundaries, so
    /// this stops new remount scheduling and makes an in-flight remount loop bail at its next step —
    /// bounding FORGET's subsequent joins to one step + one backend timeout. The state store + WARN follow
    /// in `enterVanished` (step 6). Idempotent.
    vanished_intent.store(true, std::memory_order_release);
}

void CasMountRuntime::scheduleRemount()
{
    /// Count every entry before checking whether background work is enabled. Tests can therefore observe
    /// the keeper's loss callback without depending on a recovery thread being spawned.
    schedule_remount_calls_for_test.fetch_add(1, std::memory_order_relaxed);
    if (!config.background_watermark)
        return;
    /// A terminal pool never claims/allocates/writes again (spec §3): the keeper callback must not arm a
    /// recovery thread. `remountTerminal()` covers a published terminal `Vanished` intent (`vanished_intent`,
    /// set by `publishVanishedIntent` at spec §5 step 1 for FORGET, or as `enterVanished`'s first step for a
    /// natural transition, and subsuming every settled `Vanished*` state) AND `IdentityLost` (rev.8: now a
    /// fail-loud terminal state — no demoted observer).
    if (remount_shutting_down.load() || remount_running.load() || remountTerminal())
        return;
    std::lock_guard g(remount_thread_mutex);
    if (remount_shutting_down.load() || remount_running.load() || remountTerminal())
        return;
    if (remount_thread.joinable())
        remount_thread.join();   /// Reap a previous recovery before starting a new one.
    remount_running.store(true);
    remount_thread = ThreadFromGlobalPool([this]
    {
        setThreadName(ThreadName::CAS_REMOUNT);
        uint64_t backoff_ms = 1000;
        /// Exit at any step boundary once the pool is (being driven) terminal (spec §3). `remountTerminal()`
        /// bails on a published terminal `Vanished` intent (`vanished_intent` — by FORGET at step 1, before
        /// it joins this thread, or as `enterVanished`'s first step for a natural transition, and subsuming
        /// every settled `Vanished*` state) AND on `IdentityLost` (rev.8: a fail-loud terminal state whose
        /// identity gate just set it — the thread self-exits at the next boundary, ending the observer).
        while (!remount_stop.load() && !remountTerminal())
        {
            if (remount_attempt())
                break;
            std::unique_lock lk(remount_cv_mutex);
            remount_cv.wait_for(lk, std::chrono::milliseconds(backoff_ms),
                                [this] { return remount_stop.load(); });
            backoff_ms = std::min<uint64_t>(backoff_ms * 2, 30000);
        }
        remount_running.store(false);
    });
}

bool CasMountRuntime::scheduleRemountForTest()
{
    scheduleRemount();
    std::lock_guard g(remount_thread_mutex);
    return remount_thread.joinable();
}

void CasMountRuntime::beginShutdownForTest()
{
    std::lock_guard g(remount_thread_mutex);
    remount_shutting_down.store(true);
}

void CasMountRuntime::stopRemountThread()
{
    /// Refuse further recovery arming under the same mutex used by `scheduleRemount`, before joining.
    /// Thus a keeper callback racing with teardown cannot re-arm the recovery thread after the join.
    {
        std::lock_guard g(remount_thread_mutex);
        remount_shutting_down.store(true);
    }
    /// Stop recovery first; it could otherwise recreate the keeper while the heartbeat is being retired.
    remount_stop.store(true);
    remount_cv.notify_all();
    {
        std::lock_guard g(remount_thread_mutex);
        if (remount_thread.joinable())
            remount_thread.join();
    }
}

void CasMountRuntime::finishTeardown(bool drained)
{
    /// On a drained teardown, `stop` writes an already-expired lease and the watermark farewell
    /// (`min_active = UINT64_MAX`). This lets the same server reclaim immediately while retaining the
    /// durable owner and epoch. A failure, such as another incarnation touching the slot, must not escape
    /// destruction; log it and continue teardown.
    if (mount_keeper)
    {
        if (drained)
        {
            try
            {
                mount_keeper->stop();
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("CasPool"), "CAS mount-lease: release during Pool teardown failed");
            }
        }
        else
        {
            /// If draining did not certify that every in-flight PUT resolved, a clean farewell would be
            /// false evidence. Stop background renewal without a terminal operation so the successor uses
            /// the slower but safe observation-based reclaim path.
            LOG_WARNING(getLogger("CasPool"),
                "CAS store shutdown with an unresolved ref-log PUT: skipping the clean-release marker; "
                "the next mount will treat this end as unclean");
            mount_keeper->stopBackground();
        }
    }

    /// The second join closes the residual window where a keeper loss callback observed the shutdown gate
    /// late during the heartbeat stop operation.
    {
        std::lock_guard g(remount_thread_mutex);
        if (remount_thread.joinable())
            remount_thread.join();
    }
}

}
