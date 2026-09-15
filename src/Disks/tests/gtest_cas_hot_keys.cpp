#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasHotKeys.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include "cas_test_helpers.h"

#include <Common/ProfileEvents.h>
#include <IO/S3/Client.h>

#include "config.h"

#include <Poco/Exception.h>

#include <atomic>
#include <deque>
#include <latch>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace ProfileEvents
{
    extern const Event CASHotKeyQueueWaitMicroseconds;
    extern const Event CASHotKeyCacheStarts;
    extern const Event CASHotKeyReadStarts;
    extern const Event CASHotKeyCacheVerdictsReread;
    extern const Event CASRequestGaveUp;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;

namespace
{

/// The harness's `FakeClock` is single-threaded. The lane is not: its holders sleep on the engine's
/// clock from their own threads while the test thread advances it, so every access goes through one
/// mutex. A sleep still advances the clock by what it slept, so a holder's transport backoff is real
/// time to every waiter's deadline.
struct SyncClock
{
    std::mutex mutex;
    uint64_t now = 1'000'000;
    std::vector<uint64_t> sleeps;

    std::function<uint64_t()> nowFn()
    {
        return [this] { std::lock_guard lock(mutex); return now; };
    }
    std::function<void(uint64_t)> sleepFn()
    {
        return [this](uint64_t ms) { std::lock_guard lock(mutex); sleeps.push_back(ms); now += ms; };
    }
    void advance(uint64_t ms) { std::lock_guard lock(mutex); now += ms; }
    size_t sleepCount() { std::lock_guard lock(mutex); return sleeps.size(); }
};

/// The object under test lists the tickets that wrote it, comma-separated, so order is visible.
CasHotKeys::Decide appendTicket(int ticket)
{
    return [ticket](const std::optional<Object> & current) -> std::optional<String>
    {
        if (!current)
            return std::to_string(ticket);
        return current->bytes + "," + std::to_string(ticket);
    };
}

uint64_t counter(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event].load();
}

/// A one-shot gate a write hook parks on: the first write of the key waits here until the test
/// releases it; every later write passes.
struct ParkFirstWrite
{
    std::latch parked{1};
    std::latch release{1};
    std::atomic<int> seen{0};

    void install(CountingBackend & backend, const String & key)
    {
        backend.onBeforeWrite(key, [this]
        {
            if (seen.fetch_add(1) != 0)
                return;
            parked.count_down();
            release.wait();
        });
    }
};

#if USE_AWS_S3
std::exception_ptr s3Error(Aws::S3::S3Errors code, const String & name)
{
    return std::make_exception_ptr(DB::S3Exception("the store answered " + name, code, name));
}
#endif

}

TEST(CASHotKeys, SubmissionsOfOneKeyAreSerializedInArrivalOrder)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(0);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    constexpr int N = 4;
    ParkFirstWrite park;
    park.install(*backend, "k");

    std::vector<std::thread> threads;
    std::vector<std::optional<WriteResult>> results(N);
    std::deque<std::latch> go;   /// a deque: `std::latch` is neither copyable nor movable
    for (int i = 0; i < N; ++i)
        go.emplace_back(1);
    for (int i = 0; i < N; ++i)
    {
        threads.emplace_back([&, i]
        {
            go[i].wait();
            auto op = requests.admit();
            results[i] = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(i + 1));
        });
    }
    /// The first holder is released into its write and parked there; every later thread is released
    /// only after its item is seen queued, so arrival order is the release order.
    go[0].count_down();
    park.parked.wait();
    for (int i = 1; i < N; ++i)
    {
        go[i].count_down();
        while (hot_keys.queueDepthForTest("k") < static_cast<size_t>(i + 1))
            std::this_thread::yield();
    }
    park.release.count_down();
    for (auto & t : threads)
        t.join();

    EXPECT_EQ(backend->writeCount("k"), static_cast<uint64_t>(N));
    EXPECT_EQ(backend->getCount("k"), static_cast<uint64_t>(N));   /// no cache in this task: a read per hold
    std::vector<Etag> etags;
    for (const auto & result : results)
    {
        ASSERT_TRUE(result.has_value());
        const auto * committed = std::get_if<Committed>(&*result);
        ASSERT_NE(committed, nullptr);
        etags.push_back(committed->etag);
    }
    for (size_t i = 1; i < etags.size(); ++i)
        EXPECT_FALSE(etags[i] == etags[i - 1]);
    DB::Cas::tests::expectBytes(*backend, "k", "1,2,3,4");
    auto reader = requests.admit();
    EXPECT_EQ(reader.read("k", Retry::standard())->etag, etags.back());
    EXPECT_EQ(hot_keys.laneCountForTest(), 0u);
    EXPECT_EQ(hot_keys.queueDepthForTest("k"), 0u);
}

TEST(CASHotKeys, ADecideRunsWithTheLaneMutexReleased)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(0);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    /// `queueDepthForTest` takes the lane's mutex; a `decide` run under it would deadlock this test,
    /// which hangs the whole `CAS*` gate rather than being reported by a per-test timeout. The call is
    /// the assertion.
    WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::standard()),
        [&](const std::optional<Object> &) -> std::optional<String>
        {
            EXPECT_EQ(hot_keys.queueDepthForTest("k"), 1u);
            return String("1");
        });
    EXPECT_TRUE(std::holds_alternative<Committed>(result));
}

TEST(CASHotKeys, AFailedEnqueueLeavesNoEmptyLaneBehind)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(0);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    hot_keys.enter_after_lane_hook_for_test = [] { throw std::bad_alloc(); };
    EXPECT_THROW(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1)), std::bad_alloc);
    EXPECT_EQ(hot_keys.laneCountForTest(), 0u);
    EXPECT_EQ(backend->writeCount("k"), 0u);
    hot_keys.enter_after_lane_hook_for_test = {};
    EXPECT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));
}

TEST(CASHotKeys, ResultsAreTheEnginesOwn)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(false);
    CasHotKeys hot_keys(0);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));

#if USE_AWS_S3
    /// The store refuses the bytes: the caller gets that `Refused`, at once.
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    {
        WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2));
        ASSERT_TRUE(std::holds_alternative<Refused>(result));
    }
#endif
    /// A clean refused precondition with the store unchanged: `Conflict` carrying the occupant.
    backend->refuseNextWrite("k");
    {
        WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(3));
        const auto * conflict = std::get_if<Conflict>(&result);
        ASSERT_NE(conflict, nullptr);
        EXPECT_TRUE(std::holds_alternative<Object>(conflict->seen));
        EXPECT_FALSE(conflict->any_ambiguous);
    }
    /// The resolve read fails at the transport under `once`: nothing observed. The failure is armed
    /// from a one-shot write hook, not up front, so it lands on the write's own resolve read rather
    /// than on the hold's base read, which must succeed for this sub-case to reach the write at all.
    backend->refuseNextWrite("k");
    backend->onBeforeWrite("k", [&] { backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("resolve"))); });
    {
        WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::once()), appendTicket(4));
        const auto * conflict = std::get_if<Conflict>(&result);
        ASSERT_NE(conflict, nullptr);
        EXPECT_TRUE(std::holds_alternative<NotObserved>(conflict->seen));
    }
    backend->onBeforeWrite("k", [] {});
    /// An ambiguous attempt whose resolve read fails at the transport under `once`: unresolved. Same
    /// one-shot arming as above, for the same reason.
    backend->injectAmbiguousWrite("k");
    backend->onBeforeWrite("k", [&] { backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("resolve"))); });
    {
        WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::once()), appendTicket(5));
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
        EXPECT_TRUE(gave_up->sent_any);
    }
    backend->onBeforeWrite("k", [] {});
    /// The fence trips inside the hold, before the write: nothing sent.
    bool alive = true;
    auto fenced = requests.admit([&] { return alive; });
    {
        WriteResult result = hot_keys.submit("k", fenced, fenced.freeze(Retry::standard()),
            [&](const std::optional<Object> & current) -> std::optional<String>
            {
                alive = false;
                return current->bytes + ",6";
            });
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_FALSE(gave_up->sent_any);
    }
    /// The fence trips after the landed write: the object carries the ticket, the caller is told so.
    alive = true;
    backend->onWriteCommitted("k", [&] { alive = false; });
    {
        WriteResult result = hot_keys.submit("k", fenced, fenced.freeze(Retry::standard()), appendTicket(7));
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_TRUE(gave_up->sent_any);
        DB::Cas::tests::expectBytes(*backend, "k", "1,7");
    }
    backend->onWriteCommitted("k", [] {});
    /// The engine call throws a local fault: it reaches the caller and the key is handed over.
    backend->failNextWriteWith("k", std::make_exception_ptr(std::logic_error("local")));
    EXPECT_THROW(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(8)), std::logic_error);
    EXPECT_EQ(hot_keys.laneCountForTest(), 0u);
    EXPECT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(9))));
}

TEST(CASHotKeys, ABaseReadThatFailsGivesUpAsReadModifyWriteDoes)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setAttemptTimeoutMs(1000);
    CasHotKeys hot_keys(0);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    (void)orThrow(op.create("k", "1", Retry::standard()), "seed");

    /// Enough armed failures to outlast a standard window: the read loop gives up at its deadline.
    for (int i = 0; i < 64; ++i)
        backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("read")));
    WriteResult lane = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2));
    for (int i = 0; i < 64; ++i)
        backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("read")));
    WriteResult verb = op.readModifyWrite("k", appendTicket(2), Retry::standard());

    const auto * a = std::get_if<GaveUp>(&lane);
    const auto * b = std::get_if<GaveUp>(&verb);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(a->why, b->why);
    EXPECT_EQ(a->deadline_source, b->deadline_source);
    EXPECT_EQ(a->sent_any, b->sent_any);
    EXPECT_FALSE(a->sent_any);
    EXPECT_EQ(a->last_seen.index(), b->last_seen.index());

    /// A fence that refuses the read's own reservation, and nothing smaller: the wait step passes
    /// (it asks for zero), the base read is refused before its first attempt.
    bool refuse_reservations = false;
    Fence fence{[] { return uint64_t{0}; },
                [&](uint64_t, uint64_t needed) { return refuse_reservations && needed > 0 ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
                [](uint64_t) {}};
    CasRequests fenced_requests(backend, fence, clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto fenced = fenced_requests.admit();
    refuse_reservations = true;
    WriteResult result = hot_keys.submit("k", fenced, fenced.freeze(Retry::standard()), appendTicket(3));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_FALSE(gave_up->sent_any);
}

TEST(CASHotKeys, WaitersLeaveOnTheirOwnFenceLeaseAndDeadline)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(0);
    bool lease_spent = false;
    Fence fence{[] { return uint64_t{0}; },
                [&](uint64_t, uint64_t) { return lease_spent ? Fence::Admit::NoBudget : Fence::Admit::Ok; },
                [](uint64_t) {}};
    CasRequests requests(backend, fence, clock.nowFn(), clock.sleepFn(), &hot_keys);
    ParkFirstWrite park;
    park.install(*backend, "k");

    std::thread holder([&]
    {
        auto op = requests.admit();
        (void)hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1));
    });
    park.parked.wait();

    const auto gave_up_before = counter(ProfileEvents::CASRequestGaveUp);
    /// A waiter whose own window ends while the holder is parked.
    std::optional<WriteResult> by_deadline;
    std::thread deadline_waiter([&]
    {
        auto op = requests.admit();
        by_deadline = hot_keys.submit("k", op, op.freeze(Retry::within(500)), appendTicket(2));
    });
    /// A waiter whose task stops.
    std::atomic<bool> alive{true};
    std::optional<WriteResult> by_liveness;
    std::thread liveness_waiter([&]
    {
        auto op = requests.admit([&] { return alive.load(); });
        by_liveness = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(3));
    });
    while (hot_keys.queueDepthForTest("k") < 3)
        std::this_thread::yield();

    clock.advance(600);
    deadline_waiter.join();
    alive = false;
    liveness_waiter.join();
    {
        const auto * gave_up = std::get_if<GaveUp>(&*by_deadline);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
        EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Policy);
        EXPECT_FALSE(gave_up->sent_any);
        EXPECT_EQ(gave_up->attempts_sent, 0u);
        EXPECT_TRUE(std::holds_alternative<NotObserved>(gave_up->last_seen));
    }
    {
        const auto * gave_up = std::get_if<GaveUp>(&*by_liveness);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_FALSE(gave_up->sent_any);
    }
    /// A waiter whose lease budget is gone, and whose task has stopped at the same slice: the lease
    /// speaks first, as the engine's own gate orders it. Both refusals are already in place before
    /// this thread is even spawned, so its first admission check sees both at once.
    lease_spent = true;
    std::optional<WriteResult> by_lease;
    std::thread lease_waiter([&]
    {
        auto op = requests.admit([&] { return alive.load(); });
        by_lease = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(4));
    });
    lease_waiter.join();
    {
        const auto * gave_up = std::get_if<GaveUp>(&*by_lease);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
        EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Lease);
    }
    EXPECT_EQ(counter(ProfileEvents::CASRequestGaveUp) - gave_up_before, 3u);
    EXPECT_EQ(hot_keys.queueDepthForTest("k"), 1u) << "only the parked holder remains";
    EXPECT_EQ(backend->writeCount("k"), 1u) << "no second write started";

    lease_spent = false;
    park.release.count_down();
    holder.join();
    DB::Cas::tests::expectBytes(*backend, "k", "1");
    EXPECT_EQ(hot_keys.laneCountForTest(), 0u);
}

TEST(CASHotKeys, AThrottledHolderKeepsTheWaitersQueuedThroughItsBackoff)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(0);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);

    /// The holder's own `PUT` parks here on its first attempt; once the two waiters are proven
    /// queued behind it, the release makes that attempt ambiguous instead of letting it through, so
    /// its resolve read (the key still absent) drives one reissue on the growing schedule while the
    /// waiters sit queued through it.
    std::latch parked{1};
    std::latch release{1};
    std::atomic<int> seen{0};
    backend->onBeforeWrite("k", [&]
    {
        if (seen.fetch_add(1) != 0)
            return;
        parked.count_down();
        release.wait();
        EXPECT_EQ(hot_keys.queueDepthForTest("k"), 3u);
        backend->injectAmbiguousWrite("k");
    });

    std::vector<std::thread> threads;
    std::vector<std::optional<WriteResult>> results(3);
    threads.emplace_back([&]
    {
        auto op = requests.admit();
        results[0] = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1));
    });
    parked.wait();
    /// Each waiter is spawned only once the previous one is seen queued, so arrival order -- and so
    /// the order they write in once the holder releases -- is the spawn order.
    for (int i = 1; i < 3; ++i)
    {
        threads.emplace_back([&, i]
        {
            auto op = requests.admit();
            results[i] = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(i + 1));
        });
        while (hot_keys.queueDepthForTest("k") < static_cast<size_t>(i + 1))
            std::this_thread::yield();
    }
    release.count_down();
    for (auto & t : threads)
        t.join();

    for (const auto & result : results)
        EXPECT_TRUE(std::holds_alternative<Committed>(*result));
    ASSERT_EQ(clock.sleepCount(), 1u) << "the one reissue pause, taken while the two waiters were queued";
    EXPECT_LE(clock.sleeps[0], 200u);
    EXPECT_EQ(backend->writeCount("k"), 4u) << "the ambiguous attempt counts, then three landed";
    EXPECT_EQ(hot_keys.laneCountForTest(), 0u);
    DB::Cas::tests::expectBytes(*backend, "k", "1,2,3");
}

TEST(CASHotKeys, TheNextHoldStartsFromTheLandedObjectWithoutARead)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    const auto cache_starts_before = counter(ProfileEvents::CASHotKeyCacheStarts);

    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));
    EXPECT_EQ(backend->getCount("k"), 1u);
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2))));
    EXPECT_EQ(backend->getCount("k"), 1u) << "the second hold started from the cache";
    EXPECT_EQ(counter(ProfileEvents::CASHotKeyCacheStarts) - cache_starts_before, 1u);

    /// Under `once` the one attempt is on fresh state: a read, no cached start.
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::once()), appendTicket(3))));
    EXPECT_EQ(backend->getCount("k"), 2u);
    /// `expectBytes` issues its own read, so it comes after every count assertion, not between them.
    DB::Cas::tests::expectBytes(*backend, "k", "1,2,3");
}

TEST(CASHotKeys, AnExternalWriterCostsOneResolveReadAndOneRetry)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    CasRequests external_requests = DB::Cas::tests::openRequestsForTest(backend);
    auto external = external_requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));

    const auto current = external.read("k", Retry::standard());
    (void)orThrow(external.replace("k", "E", current->etag, Retry::standard()), "external");
    const uint64_t gets_before = backend->getCount("k");
    const uint64_t writes_before = backend->writeCount("k");

    /// The caller's loop: submit, and on a conflict submit again after the flat pause.
    std::optional<WriteResult> result;
    for (int i = 0; i < 3 && !result; ++i)
    {
        WriteResult attempt = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2));
        if (std::holds_alternative<Conflict>(attempt))
            op.pause(Retry::conflictBackoff());
        else
            result = std::move(attempt);
    }
    ASSERT_TRUE(result && std::holds_alternative<Committed>(*result));
    EXPECT_EQ(backend->getCount("k") - gets_before, 1u) << "one resolve read";
    EXPECT_EQ(backend->writeCount("k") - writes_before, 2u) << "one refused write, one that landed";
    /// The submission after that starts from the cache.
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(3))));
    EXPECT_EQ(backend->getCount("k") - gets_before, 1u);
    /// `expectBytes` issues its own read, so it comes after every count assertion, not between them.
    DB::Cas::tests::expectBytes(*backend, "k", "E,2,3");
}

TEST(CASHotKeys, MalformedBytesRepairedExternallyRaiseNoCorruptionVerdict)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    CasRequests external_requests = DB::Cas::tests::openRequestsForTest(backend);
    auto external = external_requests.admit();
    /// A decide that refuses bytes it cannot decode, as the catalog's does.
    const CasHotKeys::Decide strict = [](const std::optional<Object> & current) -> std::optional<String>
    {
        if (current && current->bytes.find("garbage") != String::npos)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "not a ticket list");
        return current ? current->bytes + ",9" : String("9");
    };
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), strict)));

    auto current = external.read("k", Retry::standard());
    const Etag garbage = *orThrow(external.replace("k", "garbage", current->etag, Retry::standard()), "break");
    /// The lane's next hold starts from its cache, loses to the garbage, and remembers the garbage
    /// its resolve read saw; the caller pauses and submits again.
    WriteResult first = hot_keys.submit("k", op, op.freeze(Retry::standard()), strict);
    ASSERT_TRUE(std::holds_alternative<Conflict>(first));
    (void)orThrow(external.replace("k", "9", garbage, Retry::standard()), "repair");
    const auto reread_before = counter(ProfileEvents::CASHotKeyCacheVerdictsReread);
    /// The verdict on the cached garbage is not delivered: one read, and the decide lands on the repair.
    WriteResult second = hot_keys.submit("k", op, op.freeze(Retry::standard()), strict);
    ASSERT_TRUE(std::holds_alternative<Committed>(second));
    EXPECT_EQ(counter(ProfileEvents::CASHotKeyCacheVerdictsReread) - reread_before, 1u);
    DB::Cas::tests::expectBytes(*backend, "k", "9,9");
    /// And when the read is garbage too, that is the real corruption.
    current = external.read("k", Retry::standard());
    (void)orThrow(external.replace("k", "garbage", current->etag, Retry::standard()), "break again");
    (void)hot_keys.submit("k", op, op.freeze(Retry::standard()), strict);   /// conflict: the cache now holds garbage
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)hot_keys.submit("k", op, op.freeze(Retry::standard()), strict); });
}

TEST(CASHotKeys, ADeclineOnAHintIsRerenderedOnARead)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    CasRequests external_requests = DB::Cas::tests::openRequestsForTest(backend);
    auto external = external_requests.admit();
    /// Writes "1" once and declines while the object already says "1".
    const CasHotKeys::Decide idempotent = [](const std::optional<Object> & current) -> std::optional<String>
    {
        if (current && current->bytes == "1")
            return std::nullopt;
        return String("1");
    };
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), idempotent)));
    /// On a fresh read the decline is the caller's answer.
    {
        WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::once()), idempotent);
        const auto * declined = std::get_if<Declined>(&result);
        ASSERT_NE(declined, nullptr);
        EXPECT_TRUE(std::holds_alternative<Object>(declined->seen));
    }
    /// An external writer replaces the object; the cached hint still says "1", so the decide would
    /// decline on it. The decline is not delivered: the lane reads and the decide writes.
    const auto current = external.read("k", Retry::standard());
    (void)orThrow(external.replace("k", "0", current->etag, Retry::standard()), "external");
    const uint64_t gets_before = backend->getCount("k");
    WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::standard()), idempotent);
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    EXPECT_EQ(backend->getCount("k") - gets_before, 1u);
    DB::Cas::tests::expectBytes(*backend, "k", "1");
    /// On an absent key the decline names absence.
    WriteResult absent = hot_keys.submit("missing", op, op.freeze(Retry::standard()),
        [](const std::optional<Object> &) -> std::optional<String> { return std::nullopt; });
    const auto * declined = std::get_if<Declined>(&absent);
    ASSERT_NE(declined, nullptr);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(declined->seen));
}

TEST(CASHotKeys, TheCacheForgetsWhatItCannotVouchFor)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(false);
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));
    const auto reads = [&] { return backend->getCount("k"); };

    uint64_t before = reads();
#if USE_AWS_S3
    /// Refused: dropped, the next hold reads.
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    ASSERT_TRUE(std::holds_alternative<Refused>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2))));
    EXPECT_EQ(reads(), before);
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2))));
    EXPECT_EQ(reads(), before + 1);
#endif

    /// Ticket 2 only lands when the S3-only Refused sub-case above runs it; the terminal bytes below
    /// follow the same guard.
#if USE_AWS_S3
    constexpr auto kFinalBytes = "1,2,3,4,5,6,7";
#else
    constexpr auto kFinalBytes = "1,3,4,5,6,7";
#endif

    /// Unresolved after a send: dropped. A single-attempt submission never starts from the cache, so
    /// arming the ambiguity and the resolve-read failure up front would let the hold's own base read
    /// consume them; a one-shot write hook lands both on the write's own resolve read instead.
    backend->onBeforeWrite("k", [&]
    {
        backend->injectAmbiguousWrite("k");
        backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("resolve")));
    });
    before = reads();
    {
        WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::once()), appendTicket(3));
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
        EXPECT_TRUE(gave_up->sent_any);
    }
    backend->onBeforeWrite("k", [] {});
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(3))));
    EXPECT_EQ(reads(), before + 3);   /// the base read, the failed resolve read, the next hold's read after the entry was dropped

    /// An exception out of the write: dropped.
    backend->failNextWriteWith("k", std::make_exception_ptr(std::logic_error("local")));
    before = reads();
    EXPECT_THROW(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(4)), std::logic_error);
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(4))));
    EXPECT_EQ(reads(), before + 1);

    /// A give-up that sent nothing leaves the entry as it was: the next hold starts from it.
    bool alive = true;
    auto fenced = requests.admit([&] { return alive; });
    before = reads();
    WriteResult nothing_sent = hot_keys.submit("k", fenced, fenced.freeze(Retry::standard()),
        [&](const std::optional<Object> & current) -> std::optional<String> { alive = false; return current->bytes + ",5"; });
    ASSERT_TRUE(std::holds_alternative<GaveUp>(nothing_sent));
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(5))));
    EXPECT_EQ(reads(), before);

    /// A fill that throws after a landed write: the result stands, the next hold reads.
    hot_keys.cache_fill_hook_for_test = [] { throw std::bad_alloc(); };
    before = reads();
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(6))));
    hot_keys.cache_fill_hook_for_test = {};
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(7))));
    EXPECT_EQ(reads(), before + 1);
    DB::Cas::tests::expectBytes(*backend, "k", kFinalBytes);
}

TEST(CASHotKeys, TheBudgetBoundsBytesAndEntries)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    /// Two entries of one-byte objects weigh 2 x (1 + 1 + etag + 64); a budget of one entry and a half
    /// holds one at a time.
    auto probe_op_requests = DB::Cas::tests::openRequestsForTest(backend);
    auto probe = probe_op_requests.admit();
    const size_t etag_bytes = orThrow(probe.create("probe", "x", Retry::standard()), "probe")->render().size();
    const uint64_t one_entry = 1 + 1 + etag_bytes + 64;
    CasHotKeys hot_keys(one_entry + one_entry / 2);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    const CasHotKeys::Decide one_byte = [](const std::optional<Object> &) -> std::optional<String> { return String("x"); };

    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("a", op, op.freeze(Retry::standard()), one_byte)));
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("b", op, op.freeze(Retry::standard()), one_byte)));
    EXPECT_EQ(hot_keys.cacheEntriesForTest(), 1u) << "the older entry was evicted";
    const uint64_t gets_a = backend->getCount("a");
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("a", op, op.freeze(Retry::standard()), one_byte)));
    EXPECT_EQ(backend->getCount("a"), gets_a + 1) << "the evicted key reads";

    /// An object above the budget is not stored.
    const CasHotKeys::Decide big = [&](const std::optional<Object> &) -> std::optional<String> { return String(one_entry * 2, 'y'); };
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("c", op, op.freeze(Retry::standard()), big)));
    const uint64_t gets_c = backend->getCount("c");
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("c", op, op.freeze(Retry::standard()), big)));
    EXPECT_EQ(backend->getCount("c"), gets_c + 1);

    /// Empty objects weigh their key and their allowance: N of them stay bounded by the budget.
    CasHotKeys small(4 * one_entry);
    CasRequests small_requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &small);
    auto small_op = small_requests.admit();
    const CasHotKeys::Decide empty = [](const std::optional<Object> &) -> std::optional<String> { return String(); };
    for (int i = 0; i < 40; ++i)
        ASSERT_TRUE(std::holds_alternative<Committed>(small.submit("e" + std::to_string(i), small_op, small_op.freeze(Retry::standard()), empty)));
    EXPECT_LE(small.cacheEntriesForTest(), 4u);
}

TEST(CASHotKeys, ACachedStartPastTheDeadlineSendsNothing)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setAttemptTimeoutMs(1000);
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));

    /// The window fits the wait's zero reservation but not the write's two attempt envelopes.
    int decided = 0;
    const uint64_t writes_before = backend->writeCount("k");
    WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::within(500)),
        [&](const std::optional<Object> & current) -> std::optional<String> { ++decided; return current->bytes + ",2"; });
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_FALSE(gave_up->sent_any);
    EXPECT_EQ(decided, 1);
    EXPECT_EQ(backend->writeCount("k"), writes_before);
}

TEST(CASHotKeys, AnIdenticalCandidateLandedByAnotherServerIsTheEnginesCommit)
{
    SyncClock clock;
    auto backend = std::make_shared<CountingBackend>();
    CasHotKeys hot_keys(16ULL << 20);
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys);
    auto op = requests.admit();
    CasRequests external_requests = DB::Cas::tests::openRequestsForTest(backend);
    auto external = external_requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(1))));

    /// Another server lands exactly the candidate this hold will compute from its stale hint, and this
    /// hold's own refused write loses its answer. The resolve read finds the candidate's bytes under
    /// the moved incarnation: the engine's own rule calls that landed.
    const auto current = external.read("k", Retry::standard());
    const Etag theirs = *orThrow(external.replace("k", "1,2", current->etag, Retry::standard()), "identical");
    backend->injectAmbiguousWrite("k");
    WriteResult result = hot_keys.submit("k", op, op.freeze(Retry::standard()), appendTicket(2));
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_TRUE(committed->resolved_by_read);
    EXPECT_TRUE(committed->etag == theirs);
    DB::Cas::tests::expectBytes(*backend, "k", "1,2");
}
