#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <thread>

using namespace DB::Cas;

namespace ProfileEvents
{
extern const Event CASDetachedWorkDrainTimeouts;
}

namespace
{

/// A gate a test opens explicitly, so a task can be held in flight without a sleep.
///
/// The wait is BOUNDED and reports a failure rather than blocking for ever, and it names the gate it
/// waited on: an unbounded wait on a premise that stopped holding hung the whole binary here, which
/// hid every test that would have run after it.
struct Gate
{
    void wait(std::string_view name)
    {
        std::unique_lock lock(m);
        if (!cv.wait_for(lock, std::chrono::seconds(60), [this] { return open_; }))
            ADD_FAILURE() << "timed out waiting for '" << name << "'";
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

/// Opens its gate on every exit from the scope, so a failing assertion cannot strand the thread
/// parked behind it: the parked thread is joined during teardown, and a gate that stayed shut turned
/// a reported failure into a whole-binary deadlock.
struct GateOpenedOnExit
{
    explicit GateOpenedOnExit(Gate & gate_) : gate(gate_) {}
    GateOpenedOnExit(const GateOpenedOnExit &) = delete;
    GateOpenedOnExit & operator=(const GateOpenedOnExit &) = delete;
    ~GateOpenedOnExit() { gate.open(); }
    Gate & gate;
};

/// Completes the watched first read, then withholds its return so teardown can latch before the
/// helper is able to issue its next raw request.
class BetweenRecoveryGetsBackend : public DB::Cas::tests::OrderedFaultBackend
{
public:
    void armBetweenGets(String first_key_, std::shared_ptr<Gate> first_completed_, std::shared_ptr<Gate> release_first_)
    {
        first_key = std::move(first_key_);
        first_completed = std::move(first_completed_);
        release_first = std::move(release_first_);
        armed.store(true);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto result = DB::Cas::tests::OrderedFaultBackend::read(key, access);
        if (key == first_key && armed.exchange(false))
        {
            first_completed->open();
            release_first->wait("release_first");
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
/// recovered-frontier CAS, its first checkpoint read verifies that contribution and its second is the
/// final authority read immediately preceding materialization.
class FinalAuthorityBackend : public DB::Cas::tests::OrderedFaultBackend
{
public:
    void armFinalAuthorityRead(String checkpoint_key_)
    {
        checkpoint_key = std::move(checkpoint_key_);
        checkpoint_cas_committed.store(false);
        gets_after_checkpoint_cas.store(0);
        final_authority_returned.store(false);
        armed.store(true);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto result = DB::Cas::tests::OrderedFaultBackend::read(key, access);
        if (armed.load() && checkpoint_cas_committed.load() && key == checkpoint_key
            && gets_after_checkpoint_cas.fetch_add(1) + 1 == 2)
            final_authority_returned.store(true);
        return result;
    }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        auto result = DB::Cas::tests::OrderedFaultBackend::write(key, bytes, expected_value, access);
        /// A value is the store's committed incarnation; a `RawConflict` is a refused precondition.
        if (armed.load() && key == checkpoint_key && result.has_value())
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
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;
    return budget;
}

/// A ledger-level fixture keeps the real detached publisher but arms `CasRefLedger`'s recovery-install
/// test probe. That probe is the existing deterministic boundary after recovery materialized its
/// result and before `installRecoveryResult`.
class ManualDetachedLedger
{
public:
    ManualDetachedLedger()
        : backend(std::make_shared<FinalAuthorityBackend>())
        , mount_requests(DB::Cas::tests::openRequestsForTest(backend))
        , ledger(
              mount_requests,
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
              [] { return uint64_t{1}; },
              [] { return true; },
              [] { return uint64_t{1}; },
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
        /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the
        /// `oneAttemptBudget()` field alone; pair the two so the ledger's own admission arithmetic sees
        /// what the budget claims.
        backend->setAttemptTimeoutMs(oneAttemptBudget().attempt_timeout_ms);
        /// The engine's own retry pauses, on a clock the engine reads: one call reaches its retry
        /// deadline against a latched fault with no real time passing. `setCasRetrySleepForTest`
        /// installs the sleep on both `mount_requests` and the recovery retry loop.
        auto clock = std::make_shared<DB::Cas::tests::VirtualRetryClock>();
        mount_requests.setNowFnForTest(DB::Cas::tests::VirtualRetryClock::nowFnOf(clock));
        ledger.setCasRetrySleepForTest(DB::Cas::tests::VirtualRetryClock::sleepFnOf(clock));

        CasOperation op = mount_requests.admit();
        CasRefCatalog::initializeEmptyForNewPool(op, layout);

        /// The deterministic boundary a test pauses on: after recovery's final authority read and O(N)
        /// materialization, immediately before a materialized result installs. A no-op for every test
        /// that never arms `backend`'s final-authority read.
        ledger.setRecoveryInstallProbeForTest([this]
        {
            if (backend->finalAuthorityReturned() && !final_install_gate_claimed.exchange(true))
            {
                final_install_reached.open();
                release_final_install.wait("release_final_install");
            }
        });
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
    /// The mount plane the ledger admits every request on. Declared before it, and never moved: the
    /// ledger holds a reference to this member.
    CasRequests mount_requests;
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
        /// `connect_timeout_cap_ms` is set explicitly (not left at whatever the pool froze at open)
        /// so the drain deadline -- `attemptEnvelopeMs() + lease_safety_margin_ms` -- is really the
        /// tiny 20 ms this test wants, not 10 ms of attempt timeout plus a hidden connect-cap tax.
        CasRequestBudget budget;
        budget.attempt_timeout_ms = 10;
        budget.lease_safety_margin_ms = 10;
        budget.connect_timeout_cap_ms = 0;
        storage->poolForTest()->setDetachedDrainDeadlineBudgetForTest(budget);
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
    config.cas_request_budget.attempt_timeout_ms = 100;
    config.cas_request_budget.lease_safety_margin_ms = 100;
    /// No connect cap: the in-memory backend has no connect notion (`attemptTimeoutMs()` alone answers
    /// its budget), so the envelope must equal the bare attempt timeout for the pairing below to hold.
    config.cas_request_budget.connect_timeout_cap_ms = 0;
    /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the budget
    /// field alone; pair the two so the mount lease's admission arithmetic sees what the budget claims.
    backend->setAttemptTimeoutMs(config.cas_request_budget.attempt_timeout_ms);
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
/// Releasing that publisher meets the latched snapshot failure; zero backoff then makes settlement
/// redispatch the real token-carrying publisher, whose first action is recovery of this exact runtime.
///
/// Both faults are LATCHED and the engine's retry clock is virtual, because a write here must reach
/// its own retry deadline without committing: the engine reissues one logical write until that
/// deadline, so a counted fault the reissues outlive would let the call commit -- the parked publisher
/// would then succeed, nothing would redispatch it, and no caller would ever reach recovery.
void preparePendingRecoveryPublisher(
    const PoolPtr & store,
    const std::shared_ptr<DB::Cas::tests::OrderedFaultBackend> & backend,
    const RootNamespace & ns,
    const std::shared_ptr<Gate> & first_publisher_captured,
    const std::shared_ptr<Gate> & release_first_publisher,
    String & ckpt_key)
{
    DB::Cas::tests::VirtualRetryClock::installOn(store);

    auto capture_calls = std::make_shared<std::atomic<uint64_t>>(0);
    store->setSnapshotAfterCaptureHookForTest(
        [capture_calls, first_publisher_captured, release_first_publisher]
        {
            if (capture_calls->fetch_add(1) != 0)
                return;
            first_publisher_captured->open();
            release_first_publisher->wait("release_first_publisher");
        });

    backend->armLatchedWriteFailure("_snap/");
    ASSERT_NO_THROW(publishRef(store, ns, "ref_1", 1));
    first_publisher_captured->wait("first_publisher_captured");

    CasRequests catalog_requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation catalog_op = catalog_requests.admit();
    const auto life = CasRefCatalog::lifeIfCataloged(catalog_op, store->layout(), ns);
    ASSERT_TRUE(life);
    ckpt_key = store->layout().refCkptKey(*life);

    /// The log lands before the checkpoint conflict, leaving a real unfrontiered durable transaction.
    backend->armLatchedWriteConflict(ckpt_key);
    EXPECT_ANY_THROW(store->dropRef(ns, "ref_1"));
    backend->armLatchedWriteConflict({});
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
        release->wait("release");
    }));
    entered->wait("entered");

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
        release->wait("release");
    }));
    entered->wait("entered");

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
        release->wait("release");
        finished.store(true);
    }));
    entered->wait("entered");

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
        release->wait("release");
    }));
    entered->wait("entered");

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
        release->wait("release");
        saw_stop.store(token.stopping());
    }));
    entered->wait("entered");

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
/// The dispatched attempt is made to throw deliberately, via `snapshot_after_capture_hook_for_test`
/// (called inline, unguarded by any inner `catch`, so its throw reaches `dispatchSnapshotPublisher`'s
/// outer `catch (...)` that invokes `publish_error_hook_for_test`): with a healthy backend and no
/// injected throw the publish would succeed, the handler would never run, and this test would pass
/// while exercising nothing. An ordinary FAULTED WRITE does not reach the handler at all --
/// `tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl` treats every non-`Committed` `WriteResult`
/// (including a genuine retry give-up) as an ordinary backoff-and-retry-later outcome, a plain return,
/// never a throw -- so only an exception from OUTSIDE that write (this hook stands in for one) ever
/// reaches the handler.
///
/// The hook fires (and throws) EXACTLY ONCE, then disarms itself before throwing. An exception escaping
/// before any of the write's own failure arms now reaches the detached task's `catch (...)`, which arms
/// `advancePublishBackoff` on that exit exactly as the ordinary failure arms do, so this throw paces the
/// redispatch instead of driving it at full speed. Disarming after one throw lets the second dispatch
/// take the healthy path and settle, keeping this test's claim narrow: settlement survives ONE throwing
/// handler call.
TEST(CASDetachedWork, SettlementSurvivesAThrowingErrorHandler)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    /// Held in shared, heap-owned atomics, not plain locals: an `ASSERT_*` below can return early and
    /// skip the `stopAndDrainDetachedWork` cleanup at the bottom, and even a successful drain there only
    /// guarantees no NEW detached task starts -- an already-dispatched one can still be running and can
    /// still invoke these hooks against a frame that has already unwound.
    auto handler_ran = std::make_shared<std::atomic<bool>>(false);
    PoolConfig config;
    /// No real backoff wait: the injected throw leaves the tail over-threshold, and a REAL backoff
    /// sleep here would still be paid at teardown drain even though the test's own assertions never
    /// wait on it directly.
    config.snapshot_publish_backoff_initial_ms = 0;
    config.snapshot_publish_backoff_max_ms = 0;
    config.publish_error_hook_for_test = [handler_ran]
    {
        handler_ran->store(true);
        throw std::runtime_error("injected: the error handler itself throws");
    };
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/handler_throws"};

    auto capture_hook_ran = std::make_shared<std::atomic<bool>>(false);
    auto capture_hook_armed = std::make_shared<std::atomic<bool>>(true);
    store->setSnapshotAfterCaptureHookForTest([capture_hook_ran, capture_hook_armed]
    {
        capture_hook_ran->store(true);
        if (capture_hook_armed->exchange(false))
            throw std::runtime_error("injected: the dispatched attempt itself throws");
    });

    ASSERT_NO_THROW(publishRef(store, ns, "ref_1", 1));
    store->waitForSnapshotPublishSettleForTest(ns);
    ASSERT_TRUE(capture_hook_ran->load()) << "the dispatched attempt never reached the injected throw";
    EXPECT_TRUE(handler_ran->load()) << "the injected throw must have reached the (throwing) error handler";
    EXPECT_EQ(store->pendingSnapshotPublishesForTest(ns), 0);

    /// Same lifetime rule as below: the detached publisher reads the hooks' captured state, so it is
    /// stopped and drained before this function returns.
    ASSERT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/10000));
    store->setSnapshotAfterCaptureHookForTest(nullptr);
}

/// A publish attempt that throws BEFORE any of the ordinary write-failure arms must pace exactly like
/// an ordinary failure: settlement's only pacing gate is the publish backoff deadline, so an exception
/// that leaves it unarmed redispatches the publisher at full speed for as long as the fault persists.
/// The clock is the injected boot clock, so the schedule is virtual and the test spends no wall time
/// waiting one out; the one real-time wait is the bounded observation window, because an unpaced
/// redispatch runs on a background thread and has to be caught in the act rather than waited out.
TEST(CASDetachedWork, ThrowingPublishAttemptIsPacedByTheBackoff)
{
    auto backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    constexpr uint64_t step_ms = 100;
    /// Held in shared, heap-owned atomics, not plain locals: `stopAndDrainDetachedWork` at the bottom
    /// permanently closes admission of NEW detached tasks (via `beginTeardown`), but an `ASSERT_*` above
    /// it can return early and skip that call entirely, and even a call that runs only guarantees no new
    /// task starts -- an already-dispatched redispatch can still be running (or the drain can simply time
    /// out) and can still invoke these hooks against a frame that has already unwound.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1000);
    auto error_hook_calls = std::make_shared<std::atomic<uint64_t>>(0);
    PoolConfig config;
    config.boot_ms_fn = [fake_boot]
    {
        return fake_boot->load();
    };
    /// Initial == max, so every step of the schedule is the same virtual `step_ms` and the test can
    /// advance the clock by a constant.
    config.snapshot_publish_backoff_initial_ms = step_ms;
    config.snapshot_publish_backoff_max_ms = step_ms;
    config.publish_error_hook_for_test = [error_hook_calls]
    {
        error_hook_calls->fetch_add(1);
    };
    auto store = openPublishingPool(backend, config);
    const RootNamespace ns{"srv1/throwing_publisher_pacing"};

    auto attempts = std::make_shared<std::atomic<uint64_t>>(0);
    store->setSnapshotAfterCaptureHookForTest([attempts]
    {
        attempts->fetch_add(1);
        throw std::runtime_error("injected: every publish attempt throws before its write");
    });

    ASSERT_NO_THROW(publishRef(store, ns, "ref_1", 1));

    /// The virtual clock does not move here, so the armed deadline is still in the future for the whole
    /// window and exactly ONE attempt may have run.
    const auto observe_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < observe_until && attempts->load() <= 1)
        std::this_thread::yield();
    EXPECT_EQ(attempts->load(), 1u) << "the throwing attempt redispatched without arming the publish backoff";
    EXPECT_GE(error_hook_calls->load(), 1u) << "the injected throw never reached the error handler";

    /// One step of the schedule per iteration: the tail is still over threshold, so each mutation
    /// re-evaluates admission, and AT MOST one attempt may pass per elapsed backoff interval. At most,
    /// not exactly: a publisher dispatched by a mutation whose append has not yet returned the lane to
    /// `Ready` is refused at that gate, and the refusal arms the same backoff without the attempt ever
    /// reaching the hook below -- so a step can legitimately elapse with no attempt of its own. The
    /// regression this test exists for is the opposite, an unpaced redispatch storm, and the ceiling
    /// is what catches it; progress is asserted once after the loop.
    for (uint64_t step = 1; step <= 3; ++step)
    {
        fake_boot->fetch_add(step_ms);
        ASSERT_NO_THROW(publishRef(store, ns, "ref_" + std::to_string(step + 1), step + 1));
        /// Bounded poll rather than `waitForSnapshotPublishSettleForTest`: that call waits on a condvar
        /// predicate with no deadline, and on an unpaced-redispatch regression the reservation count
        /// never rests at zero long enough for the predicate to observe it, hanging the test instead of
        /// failing it.
        const auto settle_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (store->pendingSnapshotPublishesForTest(ns) != 0)
        {
            ASSERT_LT(std::chrono::steady_clock::now(), settle_deadline)
                << "the snapshot publish for '" << ns.string() << "' never settled: "
                << "pending_snapshot_publishes stayed nonzero";
            std::this_thread::yield();
        }
        EXPECT_LE(attempts->load(), 1 + step) << "more than one publish attempt ran within one backoff step";
    }

    /// Progress, on the injected clock so it is deterministic rather than a race with a worker: an
    /// elapsed backoff must eventually admit a further attempt, or the pacing gate would be a wedge.
    for (uint64_t extra = 0; attempts->load() < 2 && extra < 20; ++extra)
    {
        fake_boot->fetch_add(step_ms);
        ASSERT_NO_THROW(publishRef(store, ns, "ref_progress_" + std::to_string(extra), 100 + extra));
        const auto settle = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (store->pendingSnapshotPublishesForTest(ns) != 0)
        {
            ASSERT_LT(std::chrono::steady_clock::now(), settle) << "a publish never settled";
            std::this_thread::yield();
        }
    }
    EXPECT_GE(attempts->load(), 2u)
        << "no elapsed backoff ever admitted a further publish attempt: the gate is a wedge, not a pace";

    EXPECT_EQ(error_hook_calls->load(), attempts->load());
    /// The publisher is detached work: a redispatch admitted by the last elapsed backoff can still be
    /// running when this body returns, and it reads `fake_boot` through `boot_ms_fn`. Stop and drain it
    /// while the locals it reads are alive.
    ASSERT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/10000));
    store->setSnapshotAfterCaptureHookForTest(nullptr);
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
    /// Latched: the publication must meet the refusal on every reissue, or it commits and recovery
    /// never reaches its backoff.
    backend->armLatchedWriteConflict(ckpt_key);
    release_first_publisher->open();
    sleeping->wait("sleeping");

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
        release_recovery->wait("release_recovery");
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
    /// Declared AFTER the future, so it is destroyed BEFORE it: `first_recovery`'s destructor joins
    /// the recovery thread, which cannot leave the hook until this gate is open.
    GateOpenedOnExit recovery_released{*release_recovery};
    recovery_entered->wait("recovery_entered");

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
        release->wait("release");
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
    at_boundary->wait("at_boundary");
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

    CasRequests catalog_requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation catalog_op = catalog_requests.admit();
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(catalog_op, store->layout(), ns).value();
    const String base_log_key = store->layout().refLogKey(life, base_id);
    const String predecessor_key = store->layout().refLogKey(life, predecessor_seal_id);
    auto first_get_completed = std::make_shared<Gate>();
    auto release_first_get = std::make_shared<Gate>();
    backend->armBetweenGets(base_log_key, first_get_completed, release_first_get);
    release_first_publisher->open();
    first_get_completed->wait("first_get_completed");
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

    CasRequests catalog_requests = DB::Cas::tests::openRequestsForTest(fixture.backend);
    CasOperation catalog_op = catalog_requests.admit();
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(catalog_op, fixture.layout, ns).value();
    const String ckpt_key = fixture.layout.refCkptKey(life);
    /// Latched: the checkpoint write is reissued until its retry window closes, so a counted refusal
    /// the reissues outlive would let this drop commit and leave the lane Ready.
    fixture.backend->armLatchedWriteConflict(ckpt_key);
    EXPECT_ANY_THROW(fixture.ledger.dropRef(ns, "ref_1"));
    fixture.backend->armLatchedWriteConflict({});
    ASSERT_EQ(fixture.ledger.laneStateForTest(ns), RefLaneState::NeedsRecovery);

    auto detached_publisher = fixture.takeDetachedTask();
    fixture.backend->armFinalAuthorityRead(ckpt_key);
    const uint64_t installs_before = fixture.ledger.recoveryInstallCountForTest();
    auto running = std::async(std::launch::async,
        [&fixture, task = std::move(detached_publisher)]() mutable
        {
            task(DetachedStopToken(fixture.registry));
        });
    /// Released on every exit, before `running` is joined: the task parks behind this gate, and a
    /// failed assertion that left it shut deadlocked the join.
    GateOpenedOnExit final_install_released{fixture.release_final_install};

    fixture.final_install_reached.wait("final_install_reached");
    fixture.latchStop();
    fixture.release_final_install.open();
    EXPECT_NO_THROW(running.get());

    EXPECT_EQ(fixture.ledger.recoveryInstallCountForTest(), installs_before)
        << "recovery installed a materialized result after detached stop was latched";
    EXPECT_EQ(fixture.ledger.laneStateForTest(ns), RefLaneState::NeedsRecovery);
}
