#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using namespace DB;

namespace ProfileEvents
{
extern const Event PuffinFilesCacheHits;
extern const Event PuffinFilesCacheMisses;
}

namespace
{

DataLakeObjectMetadata::ExcludedRowsPtr makeExcludedRows(const std::vector<size_t> & positions)
{
    auto excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (size_t position : positions)
        excluded_rows->add(position);
    return excluded_rows;
}

}

TEST(PuffinFilesCacheMetrics, ClearDuringLoadCountsAsMissNotHit)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:///test-prefix", "puffin.bin", "etag-1", 100, 200, "data/file-a.parquet", 1, 100);
    ASSERT_TRUE(key.has_value());

    auto & counters = CurrentThread::getProfileEvents();
    const auto hits_before = counters[ProfileEvents::PuffinFilesCacheHits];
    const auto misses_before = counters[ProfileEvents::PuffinFilesCacheMisses];

    size_t load_calls = 0;
    const auto result = cache.getOrSetDeletionVector(
        *key,
        [&]()
        {
            ++load_calls;
            /// Simulate concurrent SYSTEM DROP while this key is loading: CacheBase then returns
            /// `{value, false}` because the insert token was discarded.
            cache.clear();
            return makeExcludedRows({1});
        });

    ASSERT_EQ(load_calls, 1u);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->rb_contains(1));
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits] - hits_before, 0u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses] - misses_before, 1u);

    /// Entry was not inserted after clear; the next lookup must load again.
    const auto second = cache.getOrSetDeletionVector(*key, [&]()
    {
        ++load_calls;
        return makeExcludedRows({1});
    });
    ASSERT_EQ(load_calls, 2u);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses] - misses_before, 2u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits] - hits_before, 0u);
}

TEST(PuffinFilesCacheMetrics, WaiterOfClearDiscardedLoadCountsAsMiss)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:///test-prefix", "puffin.bin", "etag-waiter", 100, 200, "data/file-w.parquet", 1, 100);
    ASSERT_TRUE(key.has_value());

    const auto hits_before = ProfileEvents::global_counters[ProfileEvents::PuffinFilesCacheHits];
    const auto misses_before = ProfileEvents::global_counters[ProfileEvents::PuffinFilesCacheMisses];

    std::promise<void> load_started;
    std::promise<void> allow_load_finish;
    std::promise<void> waiter_entered;
    auto load_started_future = load_started.get_future();
    auto allow_load_finish_future = allow_load_finish.get_future();
    auto waiter_entered_future = waiter_entered.get_future();

    std::atomic<size_t> load_calls{0};
    std::atomic<bool> waiter_load_called{false};

    std::thread producer(
        [&]()
        {
            cache.getOrSetDeletionVector(
                *key,
                [&]()
                {
                    ++load_calls;
                    load_started.set_value();
                    allow_load_finish_future.wait();
                    cache.clear();
                    return makeExcludedRows({42});
                });
        });

    ASSERT_EQ(load_started_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);

    std::thread waiter(
        [&]()
        {
            waiter_entered.set_value();
            cache.getOrSetDeletionVector(
                *key,
                [&]()
                {
                    waiter_load_called.store(true);
                    return makeExcludedRows({99});
                });
        });

    ASSERT_EQ(waiter_entered_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    /// Give the waiter time to block on the shared insert token before the producer finishes.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    allow_load_finish.set_value();
    producer.join();
    waiter.join();

    EXPECT_EQ(load_calls.load(), 1u);
    EXPECT_FALSE(waiter_load_called.load());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::PuffinFilesCacheHits] - hits_before, 0u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::PuffinFilesCacheMisses] - misses_before, 2u);
}

TEST(PuffinFilesCacheMetrics, OrdinaryHitAndMissCounters)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:///test-prefix", "puffin.bin", "etag-2", 100, 200, "data/file-b.parquet", 1, 100);
    ASSERT_TRUE(key.has_value());

    auto & counters = CurrentThread::getProfileEvents();
    const auto hits_before = counters[ProfileEvents::PuffinFilesCacheHits];
    const auto misses_before = counters[ProfileEvents::PuffinFilesCacheMisses];

    size_t load_calls = 0;
    auto load_fn = [&]()
    {
        ++load_calls;
        return makeExcludedRows({7});
    };

    ASSERT_NE(cache.getOrSetDeletionVector(*key, load_fn), nullptr);
    ASSERT_NE(cache.getOrSetDeletionVector(*key, load_fn), nullptr);

    EXPECT_EQ(load_calls, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses] - misses_before, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits] - hits_before, 1u);
}

TEST(PuffinFilesCacheMetrics, HitRemainsHitWhenCacheClearedAfterLookup)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:///test-prefix", "puffin.bin", "etag-hit-clear", 100, 200, "data/file-c.parquet", 1, 100);
    ASSERT_TRUE(key.has_value());

    auto & counters = CurrentThread::getProfileEvents();
    size_t load_calls = 0;
    auto load_fn = [&]()
    {
        ++load_calls;
        return makeExcludedRows({3});
    };

    ASSERT_NE(cache.getOrSetDeletionVector(*key, load_fn), nullptr);

    const auto hits_before = counters[ProfileEvents::PuffinFilesCacheHits];
    const auto misses_before = counters[ProfileEvents::PuffinFilesCacheMisses];

    ASSERT_NE(cache.getOrSetDeletionVector(*key, load_fn), nullptr);
    EXPECT_EQ(load_calls, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits] - hits_before, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses] - misses_before, 0u);

    /// Clearing after the hit must not rewrite the already-recorded hit as a miss. The old
    /// contains()-after-getOrSet path could race here with SYSTEM DROP PUFFIN FILES CACHE.
    cache.clear();
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits] - hits_before, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses] - misses_before, 0u);
}
