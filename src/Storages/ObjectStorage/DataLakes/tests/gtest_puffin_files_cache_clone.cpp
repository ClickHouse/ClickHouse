#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

using namespace DB;

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
extern const Metric PuffinFilesCacheFiles;
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

TEST(PuffinFilesCacheClone, CacheHitReturnsIndependentCopy)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet");
    ASSERT_TRUE(key.has_value());

    size_t load_calls = 0;
    auto load_fn = [&]()
    {
        ++load_calls;
        return makeExcludedRows({1, 5, 10});
    };

    auto first = cache.getOrSetDeletionVector(*key, load_fn);
    auto second = cache.getOrSetDeletionVector(*key, load_fn);

    ASSERT_EQ(load_calls, 1);
    ASSERT_NE(first, second);
    EXPECT_TRUE(first->rb_contains(1));
    EXPECT_TRUE(first->rb_contains(5));
    EXPECT_TRUE(first->rb_contains(10));
    EXPECT_FALSE(first->rb_contains(99));

    first->add(99);

    EXPECT_FALSE(second->rb_contains(99));

    auto third = cache.getOrSetDeletionVector(*key, load_fn);
    ASSERT_NE(third, first);
    EXPECT_FALSE(third->rb_contains(99));
    EXPECT_TRUE(third->rb_contains(10));
}

TEST(PuffinFilesCacheClone, EmptyExcludedRowsReturnsNullptr)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet");
    ASSERT_TRUE(key.has_value());

    const auto files_before = CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles);
    const auto bytes_before = CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes);

    size_t load_calls = 0;
    auto load_fn = [&]()
    {
        ++load_calls;
        return DataLakeObjectMetadata::ExcludedRowsPtr{};
    };

    auto first = cache.getOrSetDeletionVector(*key, load_fn);
    auto second = cache.getOrSetDeletionVector(*key, load_fn);

    EXPECT_EQ(first, nullptr);
    EXPECT_EQ(second, nullptr);
    EXPECT_EQ(load_calls, 1);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles), files_before + 1);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), bytes_before + 1);
}
