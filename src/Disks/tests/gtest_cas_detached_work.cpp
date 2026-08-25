#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>

using namespace DB::Cas;

namespace ProfileEvents
{
extern const Event CASDetachedWorkDrainTimeouts;
}

namespace
{

/// A gate a test opens explicitly, so a task can be held in flight without a sleep.
struct Gate
{
    void wait()
    {
        std::unique_lock lock(m);
        cv.wait(lock, [this] { return open_; });
    }
    void open()
    {
        std::lock_guard lock(m);
        open_ = true;
        cv.notify_all();
    }
    std::mutex m;
    std::condition_variable cv;
    bool open_ = false;
};

/// Completes the watched first `GET`, then withholds its return so teardown can latch before the
/// helper is able to issue its next raw request.
class BetweenRecoveryGetsBackend : public DB::Cas::tests::OrderedFaultBackend
{
public:
    using DB::Cas::tests::OrderedFaultBackend::get;

    void armBetweenGets(String first_key_, std::shared_ptr<Gate> first_completed_, std::shared_ptr<Gate> release_first_)
    {
        first_key = std::move(first_key_);
        first_completed = std::move(first_completed_);
        release_first = std::move(release_first_);
        armed.store(true);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        auto result = DB::Cas::tests::OrderedFaultBackend::get(key, range);
        if (key == first_key && armed.exchange(false))
        {
            first_completed->open();
            release_first->wait();
        }
        return result;
    }

private:
    String first_key;
    std::shared_ptr<Gate> first_completed;
    std::shared_ptr<Gate> release_first;
    std::atomic<bool> armed{false};
};

/// Identifies recovery's final authority read without changing the recovery implementation: after the
/// recovered-frontier CAS, its first checkpoint `GET` verifies that contribution and its second is the
/// final authority read immediately preceding materialization.
class FinalAuthorityBackend : public DB::Cas::tests::OrderedFaultBackend
{
public:
    using DB::Cas::tests::OrderedFaultBackend::casPut;
    using DB::Cas::tests::OrderedFaultBackend::get;

    void armFinalAuthorityRead(String checkpoint_key_)
    {
        checkpoint_key = std::move(checkpoint_key_);
        checkpoint_cas_committed.store(false);
        gets_after_checkpoint_cas.store(0);
        final_authority_returned.store(false);
        armed.store(true);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        auto result = DB::Cas::tests::OrderedFaultBackend::get(key, range);
        if (armed.load() && checkpoint_cas_committed.load() && key == checkpoint_key
            && gets_after_checkpoint_cas.fetch_add(1) + 1 == 2)
            final_authority_returned.store(true);
        return result;
    }

    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override
    {
        CasResult result = DB::Cas::tests::OrderedFaultBackend::casPut(key, bytes, expected, meta);
        if (armed.load() && key == checkpoint_key && result.outcome == CasOutcome::Committed)
            checkpoint_cas_committed.store(true);
        return result;
    }

    bool finalAuthorityReturned() const { return final_authority_returned.load(); }

private:
    String checkpoint_key;
    std::atomic<bool> armed{false};
    std::atomic<bool> checkpoint_cas_committed{false};
    std::atomic<uint64_t> gets_after_checkpoint_cas{0};
    std::atomic<bool> final_authority_returned{false};
};

CasRequestBudget oneAttemptBudget()
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 100;
    return budget;
}

/// A ledger-level fixture keeps the real detached publisher but injects its already-public mount-fence
/// callback. That callback is the existing deterministic boundary after recovery materialized its
/// result and before `installRecoveryResult`.
class ManualDetachedLedger
{
public:
    ManualDetachedLedger()
        : backend(std::make_shared<FinalAuthorityBackend>())
        , ledger(
              backend,
              layout,
              RefLedgerConfig{
                  .server_root_id = "test",
                  .gc_shards = 1,
                  .snapshot_log_count_threshold = 0,
                  .snapshot_log_bytes_threshold = 1ULL << 40,
                  .snapshot_publish_backoff_initial_ms = 0,
                  .snapshot_publish_backoff_max_ms = 0},
              event_sink,
              oneAttemptBudget(),
              "test",
              [] { return uint64_t{0}; },
              [] { return uint64_t{1}; },
              [] { return true; },
              [] { return uint64_t{1}; },
              [this](uint64_t)
              {
                  if (backend->finalAuthorityReturned() && !final_install_gate_claimed.exchange(true))
                  {
                      final_install_reached.open();
                      release_final_install.wait();
                  }
              },
              [] { return uint64_t{0}; },
              [] { return true; },
              [](const String &, const String &, const std::optional<String> &) {},
              [this](std::function<void(DetachedStopToken)> task)
              {
                  std::lock_guard lock(tasks_mutex);
                  tasks.push_back(std::move(task));
                  return true;
              },
              {},
              [](const RootNamespace &) {})
    {
        CasRefCatalog::initializeEmptyForNewPool(*backend, layout);
    }

    std::function<void(DetachedStopToken)> takeDetachedTask()
    {
        std::lock_guard lock(tasks_mutex);
        if (tasks.empty())
            throw std::logic_error("ManualDetachedLedger: no queued detached task");
        auto task = std::move(tasks.front());
        tasks.pop_front();
        return task;
    }

    void latchStop()
    {
        std::lock_guard lock(registry->mutex);
        registry->stopping = true;
        registry->cv.notify_all();
    }

    std::shared_ptr<FinalAuthorityBackend> backend;
    Layout layout{"p"};
    CasEventSink event_sink;
    std::shared_ptr<DetachedRegistryState> registry = std::make_shared<DetachedRegistryState>();
    Gate final_install_reached;
    Gate release_final_install;
    CasRefLedger ledger;

private:
    std::mutex tasks_mutex;
    std::deque<std::function<void(DetachedStopToken)>> tasks;
    std::atomic<bool> final_install_gate_claimed{false};
};

/// Spin until the drain has latched `stopping`. Bounded so a broken implementation fails the test
/// instead of hanging it.
void awaitStopLatched(const PoolPtr & store)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!store->detachedWorkStoppingForTest())
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "the drain never latched `stopping`";
        std::this_thread::yield();
    }
}

PoolPtr openPlainPool(const std::shared_ptr<InMemoryBackend> & backend, PoolConfig config = {})
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
    return Pool::open(backend, config);
}

std::shared_ptr<DB::ContentAddressedMetadataStorage> openTestStorage(bool tiny_budget = false)
{
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "cas_detached_work_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    if (tiny_budget)
    {
        storage->poolForTest()->setDetachedDrainDeadlineBudgetForTest(
            /*attempt_timeout_ms=*/10, /*lease_safety_margin_ms=*/10);
    }
    return storage;
}

/// A pool where any nonempty tail is over-threshold, so every mutation auto-dispatches a publish.
PoolPtr openPublishingPool(const std::shared_ptr<DB::Cas::tests::OrderedFaultBackend> & backend,
                           PoolConfig config = {})
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.snapshot_log_count_threshold = 0;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    /// One attempt, so a faulted PUT resolves to a definite non-committed outcome with no internal
    /// retry loop and no wall-clock wait -- the same budget the snapshot-ordering suite uses.
    config.cas_request_budget.max_attempts = 1;
    config.cas_request_budget.attempt_timeout_ms = 100;
    config.cas_request_budget.operation_deadline_ms = 5000;
    config.cas_request_budget.lease_safety_margin_ms = 100;
    return Pool::open(backend, config);
}

/// The same one-transaction publish every other ref suite drives, so a namespace reaches `Live` through
/// the REAL append lane (which is also what creates its `_ckpt`).
RefTxnId publishRef(const PoolPtr & store, const RootNamespace & ns, const String & ref, uint64_t ordinal)
{
    return store->appendRefOps(ns, MutationScope::ref(ref),
        [&ref, ordinal](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(DB::Cas::tests::namespaceBirthOp());
            for (const RefOp & op : DB::Cas::tests::publishCommittedOps(ref, ManifestRef{1, ordinal, 1}))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);
}

RefTxnId publishRef(CasRefLedger & ledger, const RootNamespace & ns, const String & ref, uint64_t ordinal)
{
    return ledger.appendRefOps(ns, MutationScope::ref(ref),
        [&ref, ordinal](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(DB::Cas::tests::namespaceBirthOp());
            for (const RefOp & op : DB::Cas::tests::publishCommittedOps(ref, ManifestRef{1, ordinal, 1}))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);
}

/// Leaves the runtime in `NeedsRecovery` while its first background publisher is held after capture.
/// Releasing that publisher consumes the armed snapshot failure; zero backoff then makes settlement
/// redispatch the real token-carrying publisher, whose first action is recovery of this exact runtime.
void preparePendingRecoveryPublisher(
    const PoolPtr & store,
    const std::shared_ptr<DB::Cas::tests::OrderedFaultBackend> & backend,
    const RootNamespace & ns,
    const std::shared_ptr<Gate> & first_publisher_captured,
    const std::shared_ptr<Gate> & release_first_publisher,
    String & ckpt_key)
{
    auto capture_calls = std::make_shared<std::atomic<uint64_t>>(0);
    store->setSnapshotAfterCaptureHookForTest(
        [capture_calls, first_publisher_captured, release_first_publisher]
        {
            if (capture_calls->fetch_add(1) != 0)
                return;
            first_publisher_captured->open();
            release_first_publisher->wait();
        });

    backend->armPutFailure("_snap/", 1);
    ASSERT_NO_THROW(publishRef(store, ns, "ref_1", 1));
    first_publisher_captured->wait();

    const auto life = CasRefCatalog::lifeIfCataloged(*backend, store->layout(), ns);
    ASSERT_TRUE(life);
    ckpt_key = store->layout().refCkptKey(*life);

    /// The log lands before the checkpoint conflict, leaving a real unfrontiered durable transaction.
    backend->armCasConflict(ckpt_key, 100);
    EXPECT_ANY_THROW(store->dropRef(ns, "ref_1"));
    backend->armCasConflict(ckpt_key, 0);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
}

}

/// A zero in-flight count must mean no tracked task still holds the pool. The hook fires at the exact
/// boundary between releasing the lease's pool reference and decrementing the count, so an
/// implementation that decrements first is caught HERE rather than by a racy post-hoc check.
TEST(CASDetachedWork, LeaseReleasesPoolBeforeDecrementing)
{
    auto backend = std::make_shared<InMemoryBackend>();

    std::weak_ptr<Pool> weak;
    std::atomic<long> use_count_at_boundary{-1};

    PoolConfig config;
    config.detached_lease_release_hook_for_test
        = [&use_count_at_boundary, &weak] { use_count_at_boundary.store(weak.use_count()); };

    auto store = openPlainPool(backend, config);
    weak = store;

    ASSERT_TRUE(store->tryDispatchDetached([](DetachedStopToken) {}));
    ASSERT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/10000));

    EXPECT_EQ(store->detachedWorkInFlightForTest(), 0u);
    EXPECT_EQ(use_count_at_boundary.load(), 1L)
        << "at the release boundary the task still held a pool reference: the lease decremented "
           "before releasing it, so a zero count does not imply the pool is free";
    EXPECT_EQ(weak.use_count(), 1L);
}

/// The drain must not RETURN while a task is still running. Asserted as "the drain is still blocked
/// while the task is held" -- the only formulation that does not race the task's completion.
TEST(CASDetachedWork, DrainDoesNotReturnWhileWorkIsInFlight)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);

    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    ASSERT_TRUE(store->tryDispatchDetached([entered, release](DetachedStopToken)
    {
        entered->open();
        release->wait();
    }));
    entered->wait();

    auto drain = std::async(std::launch::async,
                            [&store] { return store->stopAndDrainDetachedWork(/*deadline_ms=*/60000); });

    awaitStopLatched(store);
    EXPECT_EQ(drain.wait_for(std::chrono::seconds(2)), std::future_status::timeout)
        << "the drain returned while a tracked task was still in flight";

    release->open();
    EXPECT_TRUE(drain.get());
    EXPECT_EQ(store->detachedWorkInFlightForTest(), 0u);
}

/// `shutdown` must not RETURN while tracked detached work is in flight.
TEST(CASDetachedWork, ShutdownDoesNotReturnWhileWorkIsInFlight)
{
    auto storage = openTestStorage();
    auto pool = storage->poolForTest();

    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    ASSERT_TRUE(pool->tryDispatchDetached([entered, release](DetachedStopToken)
    {
        entered->open();
        release->wait();
    }));
    entered->wait();

    auto done = std::async(std::launch::async, [&storage] { storage->shutdown(); });
    awaitStopLatched(pool);
    EXPECT_EQ(done.wait_for(std::chrono::seconds(2)), std::future_status::timeout)
        << "shutdown returned while tracked detached work was still in flight";

    release->open();
    done.get();
}

/// A storage destroyed WITHOUT `shutdown` must still drain: nothing prevents that path today.
TEST(CASDetachedWork, ImplicitDestructionDrains)
{
    auto storage = openTestStorage();
    auto pool = storage->poolForTest();

    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    std::atomic<bool> finished{false};
    ASSERT_TRUE(pool->tryDispatchDetached([entered, release, &finished](DetachedStopToken)
    {
        entered->open();
        release->wait();
        finished.store(true);
    }));
    entered->wait();

    auto destroyed = std::async(std::launch::async, [&storage] { storage.reset(); });
    awaitStopLatched(pool);
    release->open();
    destroyed.get();

    EXPECT_TRUE(finished.load());
    EXPECT_EQ(pool->detachedWorkInFlightForTest(), 0u);
}

/// A storage destroyed AFTER `shutdown` must find nothing to do rather than waiting a second deadline.
TEST(CASDetachedWork, TeardownHelperIsIdempotent)
{
    auto storage = openTestStorage();
    storage->shutdown();

    const auto started = std::chrono::steady_clock::now();
    storage.reset();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(1))
        << "the second teardown repeated the wait instead of finding nothing to do";
}

/// The timeout path must be OBSERVABLE. Without this the increment could be missing entirely and every
/// other test here would stay green, because they all exercise successful drains.
TEST(CASDetachedWork, ExpiredDrainIncrementsTheTimeoutCounter)
{
    auto storage = openTestStorage(/*tiny_budget=*/true);
    auto pool = storage->poolForTest();

    /// A task that deliberately IGNORES its token, standing in for work that cannot be interrupted.
    auto release = std::make_shared<Gate>();
    auto entered = std::make_shared<Gate>();
    ASSERT_TRUE(pool->tryDispatchDetached([entered, release](DetachedStopToken)
    {
        entered->open();
        release->wait();
    }));
    entered->wait();

    const auto before = ProfileEvents::global_counters[ProfileEvents::CASDetachedWorkDrainTimeouts]
                            .load(std::memory_order_relaxed);
    storage->shutdown();
    const auto after = ProfileEvents::global_counters[ProfileEvents::CASDetachedWorkDrainTimeouts]
                           .load(std::memory_order_relaxed);
    EXPECT_EQ(after - before, 1u);

    release->open();
}

/// A task must see the stop that is already latched. The handshake matters: releasing the task before
/// the drain latches would make a CORRECT implementation record `false`.
TEST(CASDetachedWork, TaskObservesStopTokenOnceLatched)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);

    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    std::atomic<bool> saw_stop{false};

    ASSERT_TRUE(store->tryDispatchDetached([entered, release, &saw_stop](DetachedStopToken token)
    {
        entered->open();
        release->wait();
        saw_stop.store(token.stopping());
    }));
    entered->wait();

    auto drain = std::async(std::launch::async,
                            [&store] { return store->stopAndDrainDetachedWork(/*deadline_ms=*/60000); });
    awaitStopLatched(store);
    release->open();

    EXPECT_TRUE(drain.get());
    EXPECT_TRUE(saw_stop.load());
}

/// After stopping, no new detached work may be created.
TEST(CASDetachedWork, DispatchIsRefusedAfterStop)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);

    ASSERT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/10000));
    EXPECT_FALSE(store->tryDispatchDetached([](DetachedStopToken) {}));
    EXPECT_EQ(store->detachedWorkInFlightForTest(), 0u);
}

/// A dispatch that cannot allocate must leave the count untouched, not stranded above zero.
TEST(CASDetachedWork, FailedAdmissionLeavesNoStrandedCount)
{
    auto backend = std::make_shared<InMemoryBackend>();
    PoolConfig config;
    config.detached_dispatch_fault_for_test = DetachedDispatchFault::ThrowBeforeLaunch;
    auto store = openPlainPool(backend, config);

    EXPECT_ANY_THROW(store->tryDispatchDetached([](DetachedStopToken) {}));
    EXPECT_EQ(store->detachedWorkInFlightForTest(), 0u);
    EXPECT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/1000))
        << "a stranded count makes every later drain run to its full deadline";
}

/// A launch that fails after admission must roll the count back, and must not throw at its caller.
TEST(CASDetachedWork, FailedLaunchRollsBackAndDoesNotThrow)
{
    auto backend = std::make_shared<InMemoryBackend>();
    PoolConfig config;
    config.detached_dispatch_fault_for_test = DetachedDispatchFault::RefuseLaunch;
    auto store = openPlainPool(backend, config);

    EXPECT_NO_THROW(EXPECT_FALSE(store->tryDispatchDetached([](DetachedStopToken) {})));
    EXPECT_EQ(store->detachedWorkInFlightForTest(), 0u);
    EXPECT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/1000));
}

/// A dispatch that fails must not fail the mutation that triggered it, and must not strand the
/// publisher's single-flight reservation.
TEST(CASDetachedWork, FailedPublisherDispatchKeepsMutationAndClearsReservation)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    PoolConfig config;
    config.detached_dispatch_fault_for_test = DetachedDispatchFault::ThrowBeforeLaunch;
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/dispatch_fail"};

    EXPECT_NO_THROW(publishRef(store, ns, "ref_1", 1))
        << "a best-effort maintenance dispatch must never fail an otherwise-successful mutation";
    EXPECT_EQ(store->pendingSnapshotPublishesForTest(ns), 0)
        << "the reservation was stranded: quiescence and dropNamespace would wait on it forever";
}

/// Settlement must survive a throwing error handler. Today it is a bare call after the handler, so a
/// handler that throws skips it and strands the reservation for the life of the process.
///
/// The publish is FAULTED deliberately: with a healthy backend it would succeed, the handler would
/// never run, and this test would pass while exercising nothing.
TEST(CASDetachedWork, SettlementSurvivesAThrowingErrorHandler)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    PoolConfig config;
    config.publish_error_hook_for_test
        = [] { throw std::runtime_error("injected: the error handler itself throws"); };
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/handler_throws"};

    /// Arm the fault so the publisher's own PUT fails and its `catch` is entered. Use the same arming
    /// call the snapshot-ordering suite uses against this backend.
    backend->armPutFailure("_snap/", 1);

    ASSERT_NO_THROW(publishRef(store, ns, "ref_1", 1));
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(store->pendingSnapshotPublishesForTest(ns), 0);
}

/// A publisher asleep in recovery backoff must be woken by the stop, not waited out. The injected
/// sleep stands in for a long backoff without spending wall-clock time.
TEST(CASDetachedWork, StopWakesRecoveryBackoffSleep)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    PoolConfig config;
    config.snapshot_publish_backoff_initial_ms = 0;
    config.snapshot_publish_backoff_max_ms = 0;
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/backoff"};

    auto first_publisher_captured = std::make_shared<Gate>();
    auto release_first_publisher = std::make_shared<Gate>();
    String ckpt_key;
    preparePendingRecoveryPublisher(
        store, backend, ns, first_publisher_captured, release_first_publisher, ckpt_key);

    auto sleeping = std::make_shared<Gate>();
    auto release_sleep = std::make_shared<std::atomic<bool>>(false);
    store->setRefRecoveryRetrySleepForTest(
        [sleeping, release_sleep](uint64_t, const std::optional<DetachedStopToken> & token)
        {
            sleeping->open();
            while (!(token && token->stopping()) && !release_sleep->load())
                std::this_thread::yield();
        });

    /// Exhaust one checkpoint publication inside recovery so the outer retry loop enters backoff.
    backend->armCasConflict(ckpt_key, 100);
    release_first_publisher->open();
    sleeping->wait();

    const bool drained = store->stopAndDrainDetachedWork(/*deadline_ms=*/5000);
    release_sleep->store(true);
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_TRUE(drained);
}

/// A publisher parked behind another runtime's in-flight recovery is below every I/O checkpoint.
TEST(CASDetachedWork, StopWakesAConcurrentRecoveryWaiter)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    auto recovery_entered = std::make_shared<Gate>();
    auto release_recovery = std::make_shared<Gate>();
    auto recovery_hook_armed = std::make_shared<std::atomic<bool>>(false);

    PoolConfig config;
    config.snapshot_publish_backoff_initial_ms = 0;
    config.snapshot_publish_backoff_max_ms = 0;
    config.recovery_pre_first_request_hook_for_test = [recovery_entered, release_recovery, recovery_hook_armed]
    {
        if (!recovery_hook_armed->load())
            return;
        recovery_entered->open();
        release_recovery->wait();
    };
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/concurrent_recovery"};

    auto first_publisher_captured = std::make_shared<Gate>();
    auto release_first_publisher = std::make_shared<Gate>();
    String ckpt_key;
    preparePendingRecoveryPublisher(
        store, backend, ns, first_publisher_captured, release_first_publisher, ckpt_key);

    /// A synchronous caller owns the first recovery. It is deliberately not detached work, so the
    /// drain below waits only for the publisher parked behind it.
    recovery_hook_armed->store(true);
    auto first_recovery = std::async(std::launch::async, [&store, &ns] { store->listRefs(ns); });
    recovery_entered->wait();

    release_first_publisher->open();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (store->refRecoveryWaitersForTest(ns) == 0)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "no second caller reached the wait";
        std::this_thread::yield();
    }

    const bool drained = store->stopAndDrainDetachedWork(/*deadline_ms=*/5000);
    release_recovery->open();
    EXPECT_NO_THROW(first_recovery.get());
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_TRUE(drained);
}

/// The token is checked BEFORE the walk's first backend request, so a stop latched while the
/// publisher is at that boundary means the request is never issued.
///
/// A request already in flight is not interrupted. That case is a bounded drain timeout by design, so
/// the publisher is parked at the pre-request hook -- not inside a stalled `GET` -- and the drain is
/// latched before the hook is released.
TEST(CASDetachedWork, StopIsObservedBeforeTheFirstRecoveryRequest)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    auto at_boundary = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    auto recovery_hook_armed = std::make_shared<std::atomic<bool>>(false);

    PoolConfig config;
    config.snapshot_publish_backoff_initial_ms = 0;
    config.snapshot_publish_backoff_max_ms = 0;
    config.recovery_pre_first_request_hook_for_test = [at_boundary, release, recovery_hook_armed]
    {
        if (!recovery_hook_armed->load())
            return;
        at_boundary->open();
        release->wait();
    };
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/pre_first_request"};

    auto first_publisher_captured = std::make_shared<Gate>();
    auto release_first_publisher = std::make_shared<Gate>();
    String ckpt_key;
    preparePendingRecoveryPublisher(
        store, backend, ns, first_publisher_captured, release_first_publisher, ckpt_key);

    recovery_hook_armed->store(true);
    release_first_publisher->open();
    at_boundary->wait();
    const uint64_t gets_before = backend->getTotal();

    auto drain = std::async(std::launch::async,
                            [&store] { return store->stopAndDrainDetachedWork(/*deadline_ms=*/5000); });
    awaitStopLatched(store);
    release->open();

    const bool drained = drain.get();
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_TRUE(drained);
    EXPECT_EQ(backend->getTotal(), gets_before)
        << "the walk issued its first request after the stop was already latched";
}

/// `readCheckpointSnapshotBase` is one recovery boundary but performs several raw requests. Once its
/// base-log `GET` has completed, a latched stop must prevent the later predecessor-seal `GET` rather
/// than waiting until the whole helper returns.
TEST(CASDetachedWork, StopBetweenSnapshotBaseRequestsPreventsThePredecessorGet)
{
    auto backend = std::make_shared<BetweenRecoveryGetsBackend>();
    PoolConfig config;
    config.snapshot_publish_backoff_initial_ms = 0;
    config.snapshot_publish_backoff_max_ms = 0;
    auto store = openPublishingPool(backend, config);
    store->setLiveWriterEpochForTest(2);
    const RootNamespace ns{"srv1/between_snapshot_base_requests"};
    const RefTxnId birth_id{1, 1};
    const RefTxnId predecessor_seal_id{1, 2};
    const RefTxnId base_id{2, 1};

    std::vector<RefOp> birth_ops{DB::Cas::tests::namespaceBirthOp()};
    for (const RefOp & op : DB::Cas::tests::publishCommittedOps("seed_1", ManifestRef{1, 101, 1}))
        birth_ops.push_back(op);
    DB::Cas::tests::writeTxnAt(*backend, store->layout(), ns, birth_id, std::move(birth_ops));
    DB::Cas::tests::writeSealAt(*backend, store->layout(), ns, predecessor_seal_id);
    DB::Cas::tests::writeTxnAt(
        *backend,
        store->layout(),
        ns,
        base_id,
        DB::Cas::tests::publishCommittedOps("seed_2", ManifestRef{2, 101, 1}),
        predecessor_seal_id);
    DB::Cas::tests::writeRefSnapshotRaw(
        *backend,
        store->layout(),
        DB::Cas::tests::minimalLiveSnapshot(
            ns.string(),
            base_id,
            {DB::Cas::tests::committedRow("seed_1", ManifestRef{1, 101, 1}),
             DB::Cas::tests::committedRow("seed_2", ManifestRef{2, 101, 1})}));
    DB::Cas::tests::writeRecoverableCkptForRawFixture(
        *backend,
        store->layout(),
        ns,
        RefCkpt{
            .life_epoch = 1,
            .committed_through = base_id,
            .checkpoint_snapshot_id = base_id,
            .last_epoch_seal = predecessor_seal_id});
    ASSERT_NO_THROW(store->listRefs(ns));

    auto first_publisher_captured = std::make_shared<Gate>();
    auto release_first_publisher = std::make_shared<Gate>();
    String ckpt_key;
    preparePendingRecoveryPublisher(
        store, backend, ns, first_publisher_captured, release_first_publisher, ckpt_key);

    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, store->layout(), ns).value();
    const String base_log_key = store->layout().refLogKey(life, base_id);
    const String predecessor_key = store->layout().refLogKey(life, predecessor_seal_id);
    auto first_get_completed = std::make_shared<Gate>();
    auto release_first_get = std::make_shared<Gate>();
    backend->armBetweenGets(base_log_key, first_get_completed, release_first_get);
    release_first_publisher->open();
    first_get_completed->wait();
    const uint64_t predecessor_gets_before = backend->getCount(predecessor_key);

    auto drain = std::async(std::launch::async,
                            [&store] { return store->stopAndDrainDetachedWork(/*deadline_ms=*/5000); });
    awaitStopLatched(store);
    release_first_get->open();

    EXPECT_TRUE(drain.get());
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(backend->getCount(predecessor_key), predecessor_gets_before)
        << "recovery started the predecessor-seal GET after detached stop was latched";
}

/// The final fence callback runs only after the final authority read, `finish`, and O(N)
/// materialization. A stop latched there must leave the still-unrecovered runtime uninstalled.
TEST(CASDetachedWork, StopAfterRecoveryMaterializationPreventsFinalInstall)
{
    ManualDetachedLedger fixture;
    const RootNamespace ns{"srv1/stop_before_recovery_install"};
    ASSERT_NO_THROW(publishRef(fixture.ledger, ns, "ref_1", 1));

    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*fixture.backend, fixture.layout, ns).value();
    const String ckpt_key = fixture.layout.refCkptKey(life);
    fixture.backend->armCasConflict(ckpt_key, 100);
    EXPECT_ANY_THROW(fixture.ledger.dropRef(ns, "ref_1"));
    fixture.backend->armCasConflict(ckpt_key, 0);
    ASSERT_EQ(fixture.ledger.laneStateForTest(ns), RefLaneState::NeedsRecovery);

    auto detached_publisher = fixture.takeDetachedTask();
    fixture.backend->armFinalAuthorityRead(ckpt_key);
    const uint64_t installs_before = fixture.ledger.recoveryInstallCountForTest();
    auto running = std::async(std::launch::async,
        [&fixture, task = std::move(detached_publisher)]() mutable
        {
            task(DetachedStopToken(fixture.registry));
        });

    fixture.final_install_reached.wait();
    fixture.latchStop();
    fixture.release_final_install.open();
    EXPECT_NO_THROW(running.get());

    EXPECT_EQ(fixture.ledger.recoveryInstallCountForTest(), installs_before)
        << "recovery installed a materialized result after detached stop was latched";
    EXPECT_EQ(fixture.ledger.laneStateForTest(ns), RefLaneState::NeedsRecovery);
}
