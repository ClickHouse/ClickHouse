#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcReadAhead.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasDetachedWork.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

/// A disk's teardown must not wait out a GC round. The pool's teardown flag is the open request
/// plane's fence, so a round in flight is refused at its next request, its next retry sleep or its
/// next streamed refill; the joins stay and the round becomes short. These tests pin the arm, the
/// plane wiring, the sleep wiring, and the scheduler's behaviour around a round that was cut.

namespace DB::ErrorCodes
{
extern const int NETWORK_ERROR;
}

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::expectThrowsCode;

namespace
{

PoolPtr openPlainPool(const std::shared_ptr<InMemoryBackend> & backend, PoolConfig config = {})
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
    return Pool::open(backend, config);
}

/// A gate a test opens explicitly, so a thread can be held in flight without a sleep. Bounded, and it
/// names what it waited on: an unbounded wait on a premise that stopped holding hangs the binary.
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
/// parked behind it.
struct GateOpenedOnExit
{
    explicit GateOpenedOnExit(Gate & gate_) : gate(gate_) {}
    GateOpenedOnExit(const GateOpenedOnExit &) = delete;
    GateOpenedOnExit & operator=(const GateOpenedOnExit &) = delete;
    ~GateOpenedOnExit() { gate.open(); }
    Gate & gate;
};

/// A real storage over a fresh local object storage, `context == nullptr`: no system log, no
/// scheduler until the first GC entry point creates one.
std::shared_ptr<DB::ContentAddressedMetadataStorage> openTestStorage()
{
    static std::atomic<uint64_t> counter{0};
    const auto scratch = std::filesystem::temp_directory_path()
        / ("cas_gc_teardown_stop_scratch_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1)));
    auto settings = DB::Cas::tests::makeSettingsForTest("test", scratch);
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// Completes the first read of `key`, then withholds its return until released, so the round that
/// issued it is parked with that request already accounted at the backend.
class ParkFirstReadBackend : public CountingBackend
{
public:
    void armParkFirstRead(String key_, std::shared_ptr<Gate> entered_, std::shared_ptr<Gate> release_)
    {
        key = std::move(key_);
        entered = std::move(entered_);
        release = std::move(release_);
        armed.store(true);
    }

    std::optional<Raw> read(const String & read_key, TransportAccess & access) override
    {
        auto result = CountingBackend::read(read_key, access);
        if (read_key == key && armed.exchange(false))
        {
            entered->open();
            release->wait("release");
        }
        return result;
    }

    uint64_t requestsTotal() const { return getTotal() + headTotal() + listTotal() + writeTotal(); }

private:
    String key;
    std::shared_ptr<Gate> entered;
    std::shared_ptr<Gate> release;
    std::atomic<bool> armed{false};
};

/// A thread-safe sink for the scheduler's rows, with a wait that never sleeps.
class RoundLogSink
{
public:
    GcRoundLogger logger()
    {
        return [this](const GcRoundLogRecord & r)
        {
            std::lock_guard lock(mutex);
            records.push_back(r);
            cv.notify_all();
        };
    }

    std::vector<GcRoundLogRecord> all()
    {
        std::lock_guard lock(mutex);
        return records;
    }

    /// The first Finish row at index >= `from`, waiting up to `timeout`; nullopt on timeout.
    std::optional<GcRoundLogRecord> waitForFinish(size_t from, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        const auto is_finish = [&]
        {
            for (size_t i = from; i < records.size(); ++i)
                if (records[i].event_type == GcRoundLogRecord::EventType::Finish)
                    return true;
            return false;
        };
        if (!cv.wait_for(lock, timeout, is_finish))
            return std::nullopt;
        for (size_t i = from; i < records.size(); ++i)
            if (records[i].event_type == GcRoundLogRecord::EventType::Finish)
                return records[i];
        return std::nullopt;
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<GcRoundLogRecord> records;
};

size_t countStarts(const std::vector<GcRoundLogRecord> & rows)
{
    size_t n = 0;
    for (const auto & r : rows)
        if (r.event_type == GcRoundLogRecord::EventType::Start)
            ++n;
    return n;
}

}

/// The arm is idempotent, observable, and closes the door to new detached work; a drain after an
/// early arm has nothing to wait for.
TEST(CASGCTeardownStop, BeginTeardownIsIdempotentAndRefusesNewDetachedWork)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);

    EXPECT_FALSE(store->teardownBegun());
    store->beginTeardown();
    EXPECT_TRUE(store->teardownBegun());
    store->beginTeardown();
    EXPECT_TRUE(store->teardownBegun()) << "a second arm changes nothing";
    EXPECT_TRUE(store->detachedWorkStoppingForTest());

    EXPECT_FALSE(store->tryDispatchDetached([](DetachedStopToken) {}))
        << "no detached task is accepted once teardown began";
    EXPECT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/1000))
        << "the drain after an early arm finds nothing in flight and returns at once";
}

/// The open plane -- GC, FSCK, the probe -- refuses after the arm, before anything reaches the
/// backend; the mount plane, which the ref-lane drain and the farewell need alive, does not.
TEST(CASGCTeardownStop, OpenPlaneRefusesAfterTeardownBeganAndTheMountPlaneDoesNot)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    {
        CasOperation op = store->openRequests().admit();
        orThrow(op.create("p/probe", "v", Retry::once()), "create");
    }
    backend->resetCounts();

    store->beginTeardown();

    CasOperation refused = store->openRequests().admit();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)refused.read("p/probe", Retry::standard()); });
    CasOperation resumed = store->openRequests().resume(/*admitted_generation=*/0);
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)resumed.read("p/probe", Retry::standard()); });
    EXPECT_EQ(backend->getTotal(), 0u) << "a refused admission never reaches the backend";

    CasOperation mount = store->mountRequests().admit();
    ASSERT_TRUE(mount.read("p/probe", Retry::once()).has_value())
        << "the mount plane is not the open plane: teardown's own drain and farewell run on it";
    EXPECT_EQ(backend->getTotal(), 1u);
}

/// `removeManyWriteOnce` -- the verb the CAS GC bulk-delete phases call, and the one an
/// `S3ObjectStorage`-backed pool ultimately dispatches to `removeObjectsIfExistUnderProfile` -- runs on
/// the same open plane as `read` above, so a control-plane bulk delete issued after the disk's shutdown
/// (which arms teardown on this plane before the object storage's own `shutdown()` even runs, see
/// `DiskObjectStorage::shutdown()`) is refused at admission and never reaches the backend at all.
TEST(CASGCTeardownStop, RemoveManyWriteOnceIsRefusedAfterTeardownBeganAndNeverReachesTheBackend)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    {
        CasOperation op = store->openRequests().admit();
        orThrow(op.create("p/probe", "v", Retry::once()), "create");
    }
    backend->resetCounts();

    store->beginTeardown();

    const Layout layout{"p"};
    const ManifestId manifest_id{RootNamespace{"probe/ns@cas@"},
        ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1}};

    CasOperation refused = store->openRequests().admit();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        refused.removeManyWriteOnce({layout.writeOnceManifestKey(manifest_id)}, Retry::standard());
    });
    EXPECT_EQ(backend->deleteTotal(), 0u) << "a refused admission never reaches the backend, not even for one key";
}

/// The open plane's sleep is the interruptible one, in production wiring and after the test seam is
/// cleared. Arming FIRST makes this a wiring test: a predicate `wait_for` whose predicate already
/// holds returns without waiting, so a plane still wired to the plain sleep cannot pass. The deadline
/// is the assertion; no sleep orders any thread.
TEST(CASGCTeardownStop, OpenPlaneSleepReturnsAtOnceOnceTeardownBegan)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    store->beginTeardown();

    auto paused = std::async(std::launch::async, [&store] { store->openRequests().pause(60'000); });
    EXPECT_EQ(paused.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "the open plane's sleep must observe the arm; a plain sleep holds for the full minute";

    /// Clearing the seam must put the interruptible sleep back, not the engine's plain one.
    store->setCasRetrySleepForTest([](uint64_t) {});
    store->setCasRetrySleepForTest({});
    auto paused_again = std::async(std::launch::async, [&store] { store->openRequests().pause(60'000); });
    EXPECT_EQ(paused_again.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "resetting the retry-sleep seam left the open plane on the plain sleep";
}

/// A read-ahead worker resumes under the plane's generation and is refused at its first gate; the
/// fold learns it at the take site. An unconsumed future has its exception dropped by the read-ahead's
/// destructor, so the test consumes it.
TEST(CASGCTeardownStop, ReadAheadWorkerIsRefusedAndTheTakeSiteSeesIt)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    {
        CasOperation op = store->openRequests().admit();
        orThrow(op.create("p/k1", "one", Retry::once()), "create");
    }
    ThreadPool pool{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
                    CurrentMetrics::LocalThreadScheduled,
                    /*max_threads*/ 2, /*max_free_threads*/ 2, /*queue_size*/ 0};
    CasOperation op = store->openRequests().admit();
    GcReadAhead reads(op, store->openRequests(), pool, /*concurrency=*/2);

    store->beginTeardown();
    backend->resetCounts();
    reads.hintRead("p/k1");
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)reads.takeRead("p/k1"); });
    EXPECT_EQ(backend->getTotal(), 0u) << "the worker was refused before it reached the backend";
}

/// The defect itself: `shutdown` waits behind `gc_scheduler_mutex`, which a synchronous round holds
/// for its whole duration. After the fix it arms the pool first, the parked round is refused at its
/// next request, and `shutdown` returns. On the old code the arm never lands while the round is
/// parked, which is the assertion that goes red.
TEST(CASGCTeardownStop, ShutdownReturnsWhileASynchronousRoundIsParked)
{
    auto storage = openTestStorage();
    auto pool = storage->poolForTest();
    ASSERT_TRUE(pool);

    auto parked = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    std::mutex rows_mutex;
    std::vector<GcRoundLogRecord> rows;
    storage->setGcRoundRowHookForTest([&](const GcRoundLogRecord & r)
    {
        {
            std::lock_guard lock(rows_mutex);
            rows.push_back(r);
        }
        /// Park on the `lease` phase row: the round holds `gc_scheduler_mutex` and has more
        /// requests ahead of it.
        if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "lease")
        {
            parked->open();
            release->wait("release");
        }
    });

    auto round = std::async(std::launch::async, [&storage] { storage->runOneGcRoundForTest(); });
    parked->wait("parked");

    auto done = std::async(std::launch::async, [&storage] { storage->shutdown(); });

    /// The state handshake: the arm must land while the round is still parked. Bounded, and its
    /// expiry is the failure on the old code.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!pool->teardownBegun() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_TRUE(pool->teardownBegun()) << "shutdown waited for the round instead of arming the pool first";

    release->open();
    EXPECT_THROW(round.get(), DB::Exception) << "the released round must be refused at its next request";
    EXPECT_EQ(done.wait_for(std::chrono::seconds(30)), std::future_status::ready);

    std::optional<GcRoundLogRecord> finish;
    {
        std::lock_guard lock(rows_mutex);
        for (const auto & r : rows)
            if (r.event_type == GcRoundLogRecord::EventType::Finish)
                finish = r;
    }
    ASSERT_TRUE(finish.has_value());
    EXPECT_EQ(finish->outcome, GcRoundLogRecord::Outcome::Stopped);
}

/// A background round parked inside a request is refused at its NEXT request: nothing new reaches
/// the backend after the arm, the Finish row is `Stopped`, and `stop` returns with nothing in flight.
TEST(CASGCTeardownStop, BackgroundRoundIsCutAtItsNextRequest)
{
    auto backend = std::make_shared<ParkFirstReadBackend>();
    auto store = openPlainPool(backend);
    RoundLogSink sink;
    CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca", sink.logger());

    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    backend->armParkFirstRead(store->layout().gcStateKey(), entered, release);
    sched.start();
    entered->wait("entered");

    store->beginTeardown();
    const uint64_t requests_at_arm = backend->requestsTotal();
    release->open();

    const auto finish = sink.waitForFinish(/*from=*/0, std::chrono::seconds(30));
    ASSERT_TRUE(finish.has_value()) << "the parked round never finished";
    EXPECT_EQ(finish->outcome, GcRoundLogRecord::Outcome::Stopped);
    sched.stop();
    EXPECT_TRUE(sched.isQuiescent());
    EXPECT_EQ(backend->requestsTotal(), requests_at_arm)
        << "after the arm no request may reach the backend: the round unwinds at the next gate";
    EXPECT_EQ(countStarts(sink.all()), 1u) << "no further round started after the arm";
}

/// The tick queued behind a manual round must not mint a Start row after the arm: it checks the
/// flag once it holds the round mutex, before it logs anything.
TEST(CASGCTeardownStop, AQueuedScheduledTickEmitsNoStartAfterTeardownBegan)
{
    auto backend = std::make_shared<ParkFirstReadBackend>();
    auto store = openPlainPool(backend);
    RoundLogSink sink;
    /// An hour-long interval: the loop ticks only when asked.
    CasGcScheduler sched(store, std::chrono::seconds(3600), "test::gc", "ca", sink.logger());
    sched.start();

    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    backend->armParkFirstRead(store->layout().gcStateKey(), entered, release);
    auto manual = std::async(std::launch::async,
                             [&sched] { return sched.runOneRoundNow(GcRoundLogRecord::Trigger::Manual); });
    entered->wait("entered");

    /// The loop wakes and queues on the round mutex behind the parked manual round.
    sched.requestRoundSoon();
    store->beginTeardown();
    release->open();

    EXPECT_THROW((void)manual.get(), DB::Exception);
    sched.stop();
    const auto rows = sink.all();
    EXPECT_EQ(countStarts(rows), 1u) << "the queued tick minted a Start row after teardown began";
}

namespace
{

/// Arms the pool's teardown the first time a chosen prefix is listed, so the stop lands inside the
/// namespace janitor's page rather than in the round's own request chain.
class ArmOnJanitorListBackend : public CountingBackend
{
public:
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        auto page = CountingBackend::list(prefix, cursor, limit, access);
        if (!arm_prefix.empty() && prefix == arm_prefix && on_list)
        {
            on_list();
            on_list = {};
        }
        return page;
    }

    String arm_prefix;
    std::function<void()> on_list;
};

}

/// A stop that lands inside advisory work the round swallows is not `Stopped`: the deferred path
/// runs the namespace janitor's page and returns normally, and the janitor turns a refused request
/// into an anomaly. The row is `Deferred`; the round did finish. Pinned so a later change to this
/// behaviour is made on purpose.
TEST(CASGCTeardownStop, AStopInsideTheJanitorPageIsSwallowedAsDeferred)
{
    auto backend = std::make_shared<ArmOnJanitorListBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xAA};
    DB::Cas::tests::writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    DB::Cas::tests::writeManifestRaw(*backend, store->layout(), ns, r,
                                     {DB::Cas::tests::blobEntryFor("a", DB::UInt128(1))});
    DB::Cas::tests::publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, DB::UInt128(0xAB));
    const RoundReport fold_rep = gc.runRegularRound();
    ASSERT_FALSE(fold_rep.deferred) << "the first round folds";

    backend->arm_prefix = store->layout().namespaceRootPrefix();
    backend->on_list = [&store] { store->beginTeardown(); };
    RoundReport rep;
    EXPECT_NO_THROW(rep = gc.runRegularRound()) << "the janitor page swallows the refusal";
    EXPECT_TRUE(store->teardownBegun()) << "sanity: the arm landed inside the round";
    EXPECT_TRUE(rep.deferred) << "an idle second round defers; the stop inside its janitor page is advisory";
}
