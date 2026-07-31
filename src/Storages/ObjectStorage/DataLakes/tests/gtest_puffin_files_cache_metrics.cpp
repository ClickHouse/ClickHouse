#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

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

    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet", 1, 100);
    ASSERT_TRUE(key.has_value());

    auto & counters = CurrentThread::getProfileEvents();
    const auto hits_before = counters[ProfileEvents::PuffinFilesCacheHits].load();
    const auto misses_before = counters[ProfileEvents::PuffinFilesCacheMisses].load();

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
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits].load() - hits_before, 0u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses].load() - misses_before, 1u);

    /// Entry was not inserted after clear; the next lookup must load again.
    const auto second = cache.getOrSetDeletionVector(*key, [&]()
    {
        ++load_calls;
        return makeExcludedRows({1});
    });
    ASSERT_EQ(load_calls, 2u);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses].load() - misses_before, 2u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits].load() - hits_before, 0u);
}

TEST(PuffinFilesCacheMetrics, OrdinaryHitAndMissCounters)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-2", 100, 200, "data/file-b.parquet", 1, 100);
    ASSERT_TRUE(key.has_value());

    auto & counters = CurrentThread::getProfileEvents();
    const auto hits_before = counters[ProfileEvents::PuffinFilesCacheHits].load();
    const auto misses_before = counters[ProfileEvents::PuffinFilesCacheMisses].load();

    size_t load_calls = 0;
    auto load_fn = [&]()
    {
        ++load_calls;
        return makeExcludedRows({7});
    };

    ASSERT_NE(cache.getOrSetDeletionVector(*key, load_fn), nullptr);
    ASSERT_NE(cache.getOrSetDeletionVector(*key, load_fn), nullptr);

    EXPECT_EQ(load_calls, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheMisses].load() - misses_before, 1u);
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesCacheHits].load() - hits_before, 1u);
}
