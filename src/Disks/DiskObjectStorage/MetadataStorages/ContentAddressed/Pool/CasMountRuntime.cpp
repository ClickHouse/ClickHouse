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
#include <type_traits>
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
    extern const Event CASMountLeaseLost;
    extern const Event CASMountRenewalAttempts;
    extern const Event CASMountRenewalRetries;
    extern const Event CASMountRenewalResolved;
    extern const Event CASMountRenewalRecovered;
    extern const Event CASMountRenewalDeadlineExceeded;
}

namespace DB::Cas
{

void reportMountRenewProgress(const CasOverwriteProgress & progress) noexcept;
void reportMountRenewCompletion(const MountRenewResult & result) noexcept;
void configureMountRenewObservability(
    const String * server_root_id, const CasEventSink * event_sink, bool deferred) noexcept;

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
    /// FORGET publishes terminal intent before tripping the fence. That deliberate decommission is not
    /// an operational loss/recovery generation and must not pass through the transient-loss accounting
    /// edge. Ordinary external or renewal loss has no terminal intent and retains the single CAS winner.
    if (!vanishedIntentPublished())
        (void)noteLeaseLost();
}

void CasMountRuntime::tripFenceWithoutOperationalLoss()
{
    mount_fence.lost.store(true, std::memory_order_release);
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
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
    auto call = admitKeeperCall(RenewalDriverState::Dormant, RenewalDriverState::DirectCall);
    (void)renewKeeperOnce(
        std::move(call),
        RenewalDriverState::DirectCall,
        /*propagate_failure=*/true,
        /*worker_call=*/false);
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

CasMountRuntime::DriverLease::DriverLease(CasMountRuntime & runtime_, RenewalDriverState active_)
    : runtime(runtime_)
    , active(active_)
{
}

bool CasMountRuntime::renewalWorkerMayRenew() const
{
    return renewal_driver_state == RenewalDriverState::WorkerIdle
        && mount_keeper
        && mount_keeper->state() == MountLeaseKeeperState::Active;
}

CasMountRuntime::DriverLease::~DriverLease()
{
    if (finished)
        return;
    std::lock_guard lock(runtime.driver_mutex);
    if (runtime.workers_stop_requested)
        runtime.renewal_driver_state = RenewalDriverState::Stopping;
    else if (active == RenewalDriverState::WorkerCall
             && runtime.renewal_driver_state == RenewalDriverState::ParkRequested)
        runtime.renewal_driver_state = RenewalDriverState::Parked;
    else if (active == RenewalDriverState::WorkerCall)
        runtime.renewal_driver_state = RenewalDriverState::WorkerIdle;
    else if (active == RenewalDriverState::RemountCall)
        runtime.renewal_driver_state = RenewalDriverState::Parked;
    else
        runtime.renewal_driver_state = RenewalDriverState::Dormant;
    runtime.driver_cv.notify_all();
}

RenewalDriverState CasMountRuntime::DriverLease::finish(
    RenewalDriverState ordinary_destination,
    const MountRenewResult * result)
{
    std::lock_guard lock(runtime.driver_mutex);
    if (runtime.workers_stop_requested)
        runtime.renewal_driver_state = RenewalDriverState::Stopping;
    else if (active == RenewalDriverState::WorkerCall
             && runtime.renewal_driver_state == RenewalDriverState::ParkRequested)
        runtime.renewal_driver_state = RenewalDriverState::Parked;
    else
        runtime.renewal_driver_state = ordinary_destination;
    if (result && result->outcome == MountRenewOutcome::Terminal)
    {
        if (runtime.renewal_driver_state == RenewalDriverState::WorkerIdle
            || active == RenewalDriverState::DirectCall)
        {
            runtime.tripMountLost();
            runtime.schedule_remount_calls_for_test.fetch_add(1, std::memory_order_relaxed);
            if (runtime.config.background_watermark
                && !runtime.workers_stop_requested
                && !runtime.remountTerminal())
            {
                ++runtime.remount_requested_generation;
                if (active == RenewalDriverState::WorkerCall)
                    runtime.renewal_driver_state = RenewalDriverState::Parked;
            }
        }
        else if (runtime.renewal_driver_state == RenewalDriverState::Stopping
                 || active == RenewalDriverState::StartupCall)
        {
            runtime.tripFenceWithoutOperationalLoss();
        }
    }
    finished = true;
    runtime.driver_cv.notify_all();
    return runtime.renewal_driver_state;
}

CasMountRuntime::AdmittedKeeperCall CasMountRuntime::admitKeeperCall(
    RenewalDriverState required,
    RenewalDriverState active)
{
    std::lock_guard lock(driver_mutex);
    if (active == RenewalDriverState::DirectCall && config.background_watermark)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount runtime: direct renewal is disabled when background ownership is configured");
    if (renewal_driver_state != required)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount runtime: renewal driver is not admitted from the required state");
    if (!mount_keeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: renewal without a keeper");
    if (mount_keeper->state() != MountLeaseKeeperState::Active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: renewal requires an Active keeper");
    MountLeaseKeeper * keeper = mount_keeper.get();
    auto lease = std::make_unique<DriverLease>(*this, active);
    renewal_driver_state = active;
    driver_cv.notify_all();
    return AdmittedKeeperCall{std::move(lease), keeper};
}

void CasMountRuntime::installKeeper(
    UInt128 our_uuid,
    uint64_t writer_epoch,
    const std::function<uint64_t()> & now_ms)
{
    auto replacement = std::make_unique<MountLeaseKeeper>(
        backend_ptr, layout, server_root_id, our_uuid, writer_epoch,
        config.mount_lease_ttl_ms, now_ms,
        [this] { return minActive(); },
        [this](CasEvent e) { emitEvent(std::move(e)); },
        std::chrono::milliseconds(cas_request_budget.lease_safety_margin_ms),
        [this] { return bootMsNow(); });

    std::lock_guard lock(driver_mutex);
    if (renewal_driver_state != RenewalDriverState::Dormant
        && renewal_driver_state != RenewalDriverState::Parked)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount runtime: keeper replacement requires Dormant or Parked renewal ownership");
    mount_keeper = std::move(replacement);
}

uint64_t CasMountRuntime::startKeeper()
{
    RenewalDriverState active;
    RenewalDriverState destination;
    MountLeaseKeeper * keeper;
    {
        std::lock_guard lock(driver_mutex);
        if (!mount_keeper)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: startKeeper without a keeper");
        if (mount_keeper->state() != MountLeaseKeeperState::New)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: startKeeper requires a New keeper");
        if (renewal_driver_state == RenewalDriverState::Dormant)
        {
            active = RenewalDriverState::StartupCall;
            destination = RenewalDriverState::Dormant;
        }
        else if (renewal_driver_state == RenewalDriverState::Parked)
        {
            active = RenewalDriverState::RemountCall;
            destination = RenewalDriverState::Parked;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: startKeeper is not admitted in the current state");
        }
        keeper = mount_keeper.get();
        renewal_driver_state = active;
        driver_cv.notify_all();
    }

    DriverLease lease(*this, active);
    const uint64_t anchor = keeper->start();
    (void)lease.finish(destination);
    return anchor;
}

MountRenewOperationEnvironment CasMountRuntime::renewalEnvironment(bool worker_call)
{
    return MountRenewOperationEnvironment{
        .boot_ms = [this] { return bootMsNow(); },
        .stop_cause = [this, worker_call]
        {
            return config.renewal_stop_cause_for_test
                ? config.renewal_stop_cause_for_test()
                : renewalStopCause(worker_call);
        },
        .wait_before_retry = [this, worker_call](uint64_t wait_ms) { return waitForRetry(wait_ms, worker_call); },
        .observe = [](const CasOverwriteProgress & progress)
        {
            switch (progress.kind)
            {
                case CasOverwriteProgressKind::PutStarted:
                    ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalAttempts);
                    break;
                case CasOverwriteProgressKind::RetryStarted:
                    ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalRetries);
                    break;
                case CasOverwriteProgressKind::ResolvedByGet:
                    ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalResolved);
                    break;
                case CasOverwriteProgressKind::BecameAmbiguous:
                case CasOverwriteProgressKind::ResolveStarted:
                    break;
            }
            reportMountRenewProgress(progress);
        },
    };
}

CasOverwriteStopCause CasMountRuntime::renewalStopCause(bool worker_call) const
{
    std::lock_guard lock(driver_mutex);
    if (workers_stop_requested)
        return CasOverwriteStopCause::Cancelled;
    if (worker_call
        && (renewal_driver_state == RenewalDriverState::ParkRequested
            || renewal_driver_state == RenewalDriverState::Parked
            || lifecycle() != PoolLifecycle::Live
            || mount_fence.lost.load(std::memory_order_acquire)))
        return CasOverwriteStopCause::FenceOrLifecycleLost;
    return CasOverwriteStopCause::Continue;
}

bool CasMountRuntime::waitForRetry(uint64_t wait_ms, bool worker_call)
{
    std::unique_lock lock(driver_mutex);
    driver_cv.wait_for(lock, std::chrono::milliseconds(wait_ms), [this, worker_call]
    {
        return workers_stop_requested
            || (worker_call
                && (renewal_driver_state == RenewalDriverState::ParkRequested
                    || renewal_driver_state == RenewalDriverState::Parked
                    || lifecycle() != PoolLifecycle::Live
                    || mount_fence.lost.load(std::memory_order_acquire)));
    });
    if (workers_stop_requested)
        return false;
    if (worker_call
        && (renewal_driver_state == RenewalDriverState::ParkRequested
            || renewal_driver_state == RenewalDriverState::Parked
            || lifecycle() != PoolLifecycle::Live
            || mount_fence.lost.load(std::memory_order_acquire)))
        return false;
    return true;
}

void CasMountRuntime::consumeRenewResult(
    const MountRenewResult & result,
    RenewalDriverState active_state,
    RenewalDriverState returned_state,
    bool propagate_failure)
{
    /// Driver ownership has already been restored by `DriverLease::finish`; this is the single logical
    /// consumption boundary and it runs without `driver_mutex` or keeper access.
    if (result.outcome == MountRenewOutcome::Committed
        && (result.diagnostics.attempts_sent > 1 || result.diagnostics.resolved_by_get))
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalRecovered);
    if (result.outcome == MountRenewOutcome::Terminal
        && result.diagnostics.deadline_source == CasOverwriteDeadlineSource::ExternalLeaseSafety
        && result.diagnostics.stop_cause == CasOverwriteStopCause::Continue
        && (result.diagnostics.unresolved_reason == CasUnresolvedReason::NoAttemptSent
            || result.diagnostics.unresolved_reason == CasUnresolvedReason::DeadlineMidWay))
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalDeadlineExceeded);

    if (result.outcome == MountRenewOutcome::Committed)
    {
        const uint64_t ttl_ms = static_cast<uint64_t>(config.mount_lease_ttl_ms.count());
        setMountDeadline(
            result.attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
                ? std::numeric_limits<uint64_t>::max()
                : result.attempt_start_boot_ms + ttl_ms);
        reportMountRenewCompletion(result);
        return;
    }
    if (result.outcome == MountRenewOutcome::NotAttempted)
    {
        reportMountRenewCompletion(result);
        return;
    }

    if (!result.failure)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: terminal renewal has no failure");
    try
    {
        std::rethrow_exception(result.failure);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw;
    }
    catch (...)
    {
    }

    reportMountRenewCompletion(result);

    (void)active_state;
    (void)returned_state;

    if (propagate_failure)
        std::rethrow_exception(result.failure);
}

uint64_t CasMountRuntime::renewKeeperOnce(
    AdmittedKeeperCall call,
    RenewalDriverState active,
    bool propagate_failure,
    bool worker_call)
{
    /// Configuration is pointer/POD-only. A parked redo retains its completed observation for the
    /// whole-chain finalizer to deliver after `remount_mutex` is released.
    configureMountRenewObservability(
        &server_root_id, &event_sink, active == RenewalDriverState::RemountCall);
    const MountRenewResult result = call.keeper->renew(cas_request_budget, renewalEnvironment(worker_call));
    const RenewalDriverState destination = active == RenewalDriverState::WorkerCall
        ? RenewalDriverState::WorkerIdle
        : (active == RenewalDriverState::RemountCall ? RenewalDriverState::Parked : RenewalDriverState::Dormant);
    const RenewalDriverState returned_state = call.lease->finish(destination, &result);
    if (result.outcome == MountRenewOutcome::Terminal && config.renewal_terminal_deposited_hook_for_test)
        config.renewal_terminal_deposited_hook_for_test();
    consumeRenewResult(result, active, returned_state, propagate_failure);
    return result.attempt_start_boot_ms;
}

uint64_t CasMountRuntime::renewKeeperForStartupOnce()
{
    auto call = admitKeeperCall(RenewalDriverState::Dormant, RenewalDriverState::StartupCall);
    return renewKeeperOnce(
        std::move(call),
        RenewalDriverState::StartupCall,
        /*propagate_failure=*/true,
        /*worker_call=*/false);
}

uint64_t CasMountRuntime::renewKeeperForRemountOnce()
{
    auto call = admitKeeperCall(RenewalDriverState::Parked, RenewalDriverState::RemountCall);
    return renewKeeperOnce(
        std::move(call),
        RenewalDriverState::RemountCall,
        /*propagate_failure=*/true,
        /*worker_call=*/false);
}

void CasMountRuntime::keeperReset()
{
    std::lock_guard lock(driver_mutex);
    if (renewal_driver_state != RenewalDriverState::Dormant
        && renewal_driver_state != RenewalDriverState::Parked)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: keeper reset while renewal is active");
    mount_keeper.reset();
}

ThreadFromGlobalPool CasMountRuntime::makeWorker(std::function<void()> body)
{
    if (config.worker_factory)
        return config.worker_factory(std::move(body));
    return ThreadFromGlobalPool(std::move(body));
}

void CasMountRuntime::startBackgroundWorkers(std::chrono::milliseconds period)
{
    {
        std::lock_guard lock(driver_mutex);
        if (renewal_driver_state != RenewalDriverState::Dormant
            || workers_starting || workers_started
            || renewal_worker.joinable() || remount_worker.joinable())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: background workers cannot start in the current state");
        if (!mount_keeper || mount_keeper->state() != MountLeaseKeeperState::Active)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: background workers require an Active keeper");
        workers_starting = true;
        workers_stop_requested = false;
        worker_loops_released = false;
        renewal_period = period;
        renewal_driver_state = RenewalDriverState::WorkerIdle;
    }

    ThreadFromGlobalPool first;
    ThreadFromGlobalPool second;
    try
    {
        first = makeWorker([this] { renewalLoop(); });
        second = makeWorker([this] { remountLoop(); });
    }
    catch (...)
    {
        {
            std::lock_guard lock(driver_mutex);
            workers_stop_requested = true;
            worker_loops_released = true;
            renewal_driver_state = RenewalDriverState::Stopping;
            driver_cv.notify_all();
        }
        if (first.joinable())
            first.join();
        if (second.joinable())
            second.join();
        {
            std::lock_guard lock(driver_mutex);
            workers_starting = false;
            workers_started = false;
            renewal_driver_state = RenewalDriverState::Dormant;
        }
        tripFenceWithoutOperationalLoss();
        throw;
    }

    {
        std::lock_guard lock(driver_mutex);
        renewal_worker = std::move(first);
        remount_worker = std::move(second);
        workers_starting = false;
        workers_started = true;
        worker_loops_released = true;
        driver_cv.notify_all();
    }
}

void CasMountRuntime::renewalLoop()
{
    setThreadName(ThreadName::CAS_LEASE_KEEPER);
    {
        std::unique_lock lock(driver_mutex);
        driver_cv.wait(lock, [this] { return worker_loops_released; });
        if (workers_stop_requested || remountTerminal())
            return;
    }

    while (true)
    {
        if (config.renewal_before_driver_lock_hook_for_test)
            config.renewal_before_driver_lock_hook_for_test();

        AdmittedKeeperCall call;
        {
            std::unique_lock lock(driver_mutex);
            if (workers_stop_requested || remountTerminal())
                return;
            if (renewal_driver_state == RenewalDriverState::ParkRequested)
            {
                renewal_driver_state = RenewalDriverState::Parked;
                driver_cv.notify_all();
            }
            if (!renewalWorkerMayRenew())
            {
                driver_cv.wait(lock, [this]
                {
                    const bool terminal = remountTerminal();
                    if (!workers_stop_requested
                        && !terminal
                        && !renewalWorkerMayRenew()
                        && config.renewal_parked_predicate_false_hook_for_test)
                        config.renewal_parked_predicate_false_hook_for_test();
                    return workers_stop_requested || terminal || renewalWorkerMayRenew();
                });
                if (workers_stop_requested || remountTerminal())
                    return;
                continue;
            }

            const uint64_t last_anchor = mount_keeper->lastCommittedAttemptStartBootMs();
            const uint64_t period_ms = static_cast<uint64_t>(std::max<int64_t>(0, renewal_period.count()));
            const uint64_t due = last_anchor > std::numeric_limits<uint64_t>::max() - period_ms
                ? std::numeric_limits<uint64_t>::max()
                : last_anchor + period_ms;
            const uint64_t now = bootMsNow();
            if (now < due)
            {
                /// Every runtime notification can change the cadence decision: park/resume may happen
                /// entirely while this worker is idle, and a remount may publish an already-overdue
                /// keeper anchor. Re-sample state and BOOTTIME after any wake instead of retaining the
                /// old relative wait until its wall-clock timeout.
                driver_cv.wait_for(lock, std::chrono::milliseconds(due - now));
                continue;
            }
            MountLeaseKeeper * keeper = mount_keeper.get();
            auto lease = std::make_unique<DriverLease>(*this, RenewalDriverState::WorkerCall);
            renewal_driver_state = RenewalDriverState::WorkerCall;
            driver_cv.notify_all();
            call = AdmittedKeeperCall{std::move(lease), keeper};
        }

        if (config.renewal_admitted_hook_for_test)
            config.renewal_admitted_hook_for_test();
        try
        {
            (void)renewKeeperOnce(
                std::move(call),
                RenewalDriverState::WorkerCall,
                /*propagate_failure=*/false,
                /*worker_call=*/true);
        }
        catch (...)
        {
            /// The worker path does not propagate renewal failures, so anything arriving here is this
            /// loop's own state machine reporting that it was driven out of contract. A background loop
            /// must not take the process down, but it must not keep driving a state machine that just
            /// proved wrong either: every later renewal would be unaudited.
            ///
            /// Exiting is safe because write admission is bounded by the fence deadline, not by this
            /// thread's existence -- `mayMutate` requires `bootMsNow() < mount_fence.deadline_boot_ms`, so
            /// writes stop being admitted within one TTL whether or not anyone is renewing. Tripping the
            /// fence first brings that boundary forward instead of waiting for the TTL to lapse.
            ///
            /// Residual, deliberately not handled here: `scheduleRemount` also has an external caller, so
            /// a later remount can still re-arm the fence and buy another bounded TTL. That makes the pool
            /// flap rather than settle. Pairing this exit with a terminal publication would settle it, but
            /// which terminal state means "this runtime's own driver broke" is a user-visible choice that
            /// does not belong in a rescue path.
            tripMountLost();
            tryLogCurrentException(getLogger("CasPool"), "CAS mount-lease renewal loop");
            chassert(false);
            return;
        }
    }
}

void CasMountRuntime::remountLoop()
{
    setThreadName(ThreadName::CAS_REMOUNT);
    {
        std::unique_lock lock(driver_mutex);
        driver_cv.wait(lock, [this] { return worker_loops_released; });
        if (workers_stop_requested || remountTerminal())
            return;
    }

    uint64_t backoff_ms = 1000;
    while (true)
    {
        uint64_t snapshot;
        {
            std::unique_lock lock(driver_mutex);
            driver_cv.wait(lock, [this]
            {
                return workers_stop_requested
                    || remountTerminal()
                    || remount_requested_generation > remount_handled_generation;
            });
            if (workers_stop_requested || remountTerminal())
                return;
            snapshot = remount_requested_generation;

            if (renewal_driver_state == RenewalDriverState::WorkerCall)
                renewal_driver_state = RenewalDriverState::ParkRequested;
            else if (renewal_driver_state == RenewalDriverState::WorkerIdle)
                renewal_driver_state = RenewalDriverState::Parked;
            driver_cv.notify_all();
            driver_cv.wait(lock, [this]
            {
                return workers_stop_requested || remountTerminal()
                    || renewal_driver_state == RenewalDriverState::Parked;
            });
            if (workers_stop_requested || remountTerminal())
                return;
            if (config.remount_parked_hook_for_test)
                config.remount_parked_hook_for_test();
        }

        bool recovered = false;
        try
        {
            recovered = remount_attempt();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CasPool"), "CAS self-remount attempt failed");
        }

        if (!recovered)
        {
            std::unique_lock lock(driver_mutex);
            if (remountTerminal())
                return;
            driver_cv.wait_for(lock, std::chrono::milliseconds(backoff_ms), [this]
            {
                return workers_stop_requested || remountTerminal();
            });
            if (workers_stop_requested || remountTerminal())
                return;
            backoff_ms = std::min<uint64_t>(backoff_ms * 2, 30000);
            continue;
        }

        backoff_ms = 1000;
        {
            std::lock_guard lock(driver_mutex);
            if (workers_stop_requested || remountTerminal())
                return;
            remount_handled_generation = std::max(remount_handled_generation, snapshot);
            if (remount_requested_generation > remount_handled_generation)
                continue;
            if (lifecycle() == PoolLifecycle::Live
                && mount_keeper
                && mount_keeper->state() == MountLeaseKeeperState::Active)
            {
                renewal_driver_state = RenewalDriverState::WorkerIdle;
                driver_cv.notify_all();
            }
        }
    }
}

void CasMountRuntime::stopBackgroundWorkers()
{
    ThreadFromGlobalPool * renewal_to_join = nullptr;
    ThreadFromGlobalPool * remount_to_join = nullptr;
    {
        std::lock_guard lock(driver_mutex);
        if (!workers_started && !workers_starting
            && !renewal_worker.joinable() && !remount_worker.joinable())
            return;
        workers_stop_requested = true;
        worker_loops_released = true;
        renewal_driver_state = RenewalDriverState::Stopping;
        driver_cv.notify_all();
        renewal_to_join = &renewal_worker;
        remount_to_join = &remount_worker;
    }

    if (renewal_to_join->joinable())
        renewal_to_join->join();
    if (remount_to_join->joinable())
        remount_to_join->join();

    {
        std::lock_guard lock(driver_mutex);
        workers_starting = false;
        workers_started = false;
        renewal_driver_state = RenewalDriverState::Dormant;
        driver_cv.notify_all();
    }
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

bool CasMountRuntime::noteLeaseLost()
{
    /// `Live -> TransientNotLive`, and nothing else. A compare-exchange FROM `Live` leaves every other
    /// state untouched, so a terminal state is never downgraded and a repeated call is a no-op. This is
    /// the only transition the runtime renewal consumer performs, and it needs no lock because of that discipline.
    PoolLifecycle expected = PoolLifecycle::Live;
    if (pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::TransientNotLive, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        /// The `since` the lifecycle snapshot reports for `not_live` — the wall-clock instant this became
        /// non-`Live`. Only the winning transition writes it (the guard above), so it is not re-stamped.
        lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);
        /// This transition can be reached while the renewal driver or remount serializer lock is
        /// held. Trace-profile collection may allocate and enqueue a stack trace, so the lock-safe
        /// observability path must remain the direct atomic increment.
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountLeaseLost);
        return true;
    }
    return false;
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

std::unique_lock<std::mutex> CasMountRuntime::lockTerminalPublication()
{
    if (config.terminal_publication_waiting_for_driver_lock_hook_for_test)
        config.terminal_publication_waiting_for_driver_lock_hook_for_test();

    std::unique_lock lock(driver_mutex, std::try_to_lock);
    if (!lock.owns_lock())
    {
        if (config.terminal_publication_driver_lock_contended_hook_for_test)
            config.terminal_publication_driver_lock_contended_hook_for_test();
        lock.lock();
    }

    if (config.terminal_publication_driver_lock_acquired_hook_for_test)
        config.terminal_publication_driver_lock_acquired_hook_for_test();
    return lock;
}

void CasMountRuntime::enterIdentityLost()
{
    /// `TransientNotLive -> IdentityLost`, one way. The compare-exchange FROM `TransientNotLive` gives
    /// the brief's "from TransientNotLive only" precondition, idempotency (a second call finds the state
    /// already `IdentityLost` and its exchange fails), and safety against a concurrent keeper
    /// `noteLeaseLost` (which only ever moves `Live -> TransientNotLive`, never away from it). It does NOT
    /// set `vanished_intent` (that latch is reserved for the `Vanished*` idempotency/FORGET protocol);
    /// rev.8 makes `IdentityLost` a fail-loud TERMINAL state through `remountTerminal`, which folds it
    /// into the worker-exit boundary alongside `vanished_intent`, so the remount/GC workers
    /// self-exit rather than demote.
    /// `since` for the `identity_lost` snapshot row — the wall-clock instant the observer proved the
    /// sentinels gone. Stamped (release) BEFORE the CAS that publishes `IdentityLost`, so a reader that
    /// acquire-observes `IdentityLost` is guaranteed to observe this timestamp too (the winning CAS's
    /// release carries this prior store) — the same before-publish ordering `enterVanished` uses. Safe to
    /// stamp before the CAS here, unlike the lock-free `noteLeaseLost`: this runs only from
    /// `TransientNotLive` under `Pool::remount_mutex` (a `Vanished` pool bailed at the caller's
    /// `isVanished` gate and the caller guards `!= IdentityLost`), so the CAS wins deterministically and
    /// the stamp can never land on a state we did not transition.
    bool transitioned = false;
    {
        auto lock = lockTerminalPublication();
        lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);

        PoolLifecycle expected = PoolLifecycle::TransientNotLive;
        transitioned = pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::IdentityLost, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    if (!transitioned)
        return;

    driver_cv.notify_all();

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

    if (terminal_state_published.load(std::memory_order_acquire))
        return;

    if (config.vanished_reason_prepare_hook_for_test)
        config.vanished_reason_prepare_hook_for_test();
    String prepared_reason = reason;
    const int64_t prepared_since_wall_s = wallClockNowSeconds();
    static_assert(std::is_nothrow_move_assignable_v<String>);

    bool transitioned = false;
    /// The guard is published LAST. Reason preparation above is the only potentially-throwing step;
    /// once this block begins, the statically-proven-noexcept move and atomic stores either publish
    /// one complete terminal transition or observe that an earlier transition already completed.
    {
        auto lock = lockTerminalPublication();
        if (!terminal_state_published.load(std::memory_order_acquire))
        {
            transitioned = true;

            /// Publish the terminal-intent latch (spec §3). For a natural transition this is the FIRST
            /// publish; for FORGET, `publishVanishedIntent` already set it at step 1. Either way it is
            /// published before the state store below and while holding the mutex used by every
            /// `driver_cv` terminal predicate.
            vanished_intent.store(true, std::memory_order_release);

            /// Record the reason BEFORE the state's release-store, so a reader that acquire-observes the
            /// terminal state (e.g. `Pool::throwIfLifecycleTerminal`) also observes this string. Written
            /// exactly once.
            vanished_reason = std::move(prepared_reason);

            /// The `since` the lifecycle snapshot reports for the `vanished` row — the wall-clock instant
            /// of the terminal transition. Written before the `pool_lifecycle` release-store below, same
            /// as the reason, so an acquire-observer of the terminal state also observes it.
            lifecycle_since_wall_s.store(prepared_since_wall_s, std::memory_order_release);

            /// The driver mutex serializes terminal transitions, and no non-terminal transition can move
            /// a `Vanished` state (their compare-exchanges are keyed on `Live`/`TransientNotLive`), so this
            /// value is absorbing.
            pool_lifecycle.store(which, std::memory_order_release);
            terminal_state_published.store(true, std::memory_order_release);
        }
    }

    driver_cv.notify_all();
    if (!transitioned)
        return;

    ProfileEvents::increment(ProfileEvents::CASDataRootVanished);
    LOG_WARNING(getLogger("CasPool"),
        "Content-addressed pool '{}' entered Vanished({}): {}. The disk stays registered but store-class "
        "access now fails loud with a typed error (truth); restart re-registers the name.",
        server_root_id, label, reason);
}

void CasMountRuntime::publishVanishedIntent()
{
    /// spec §5 step 1: publish the terminal-intent latch WITHOUT settling the state. The terminal consumer
    /// and the remount loop both consult `vanished_intent` at their step boundaries, so
    /// this stops new remount scheduling and makes an in-flight remount loop bail at its next step —
    /// bounding FORGET's subsequent joins to one step + one backend timeout. The state store + WARN follow
    /// in `enterVanished` (step 6). Idempotent.
    {
        auto lock = lockTerminalPublication();
        vanished_intent.store(true, std::memory_order_release);
    }
    driver_cv.notify_all();
}

void CasMountRuntime::scheduleRemount()
{
    schedule_remount_calls_for_test.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard lock(driver_mutex);
    if (workers_stop_requested || remountTerminal())
        return;
    ++remount_requested_generation;
    if (renewal_driver_state == RenewalDriverState::WorkerCall)
        renewal_driver_state = RenewalDriverState::ParkRequested;
    else if (renewal_driver_state == RenewalDriverState::WorkerIdle)
        renewal_driver_state = RenewalDriverState::Parked;
    driver_cv.notify_all();
}

bool CasMountRuntime::scheduleRemountForTest()
{
    scheduleRemount();
    std::lock_guard lock(driver_mutex);
    return workers_started && remount_requested_generation > remount_handled_generation;
}

void CasMountRuntime::beginShutdownForTest()
{
    std::lock_guard lock(driver_mutex);
    workers_stop_requested = true;
    renewal_driver_state = RenewalDriverState::Stopping;
    driver_cv.notify_all();
}

RenewalDriverState CasMountRuntime::renewalDriverStateForTest() const
{
    std::lock_guard lock(driver_mutex);
    return renewal_driver_state;
}

void CasMountRuntime::waitForRenewalDriverStateForTest(RenewalDriverState expected) const
{
    std::unique_lock lock(driver_mutex);
    if (!driver_cv.wait_for(lock, std::chrono::seconds(20), [this, expected]
        {
            return renewal_driver_state == expected;
        }))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: timed out waiting for renewal driver state");
}

bool CasMountRuntime::workersRunningForTest() const
{
    std::lock_guard lock(driver_mutex);
    return workers_started && renewal_worker.joinable() && remount_worker.joinable();
}

uint64_t CasMountRuntime::remountRequestedGenerationForTest() const
{
    std::lock_guard lock(driver_mutex);
    return remount_requested_generation;
}

void CasMountRuntime::finishTeardown(bool drained)
{
    stopBackgroundWorkers();

    if (!mount_keeper)
        return;
    if (drained && mount_keeper->state() == MountLeaseKeeperState::Active)
    {
        try
        {
            mount_keeper->release();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CasPool"), "CAS mount-lease: release during Pool teardown failed");
        }
    }
    else if (!drained)
    {
        LOG_WARNING(
            getLogger("CasPool"),
            "CAS store shutdown with an unresolved ref-log PUT: skipping the clean-release marker; "
            "the next mount will treat this end as unclean");
    }
}
}
