#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/CurrentMetrics.h>
#include <Storages/MergeTree/ColumnsCache.h>

namespace CurrentMetrics
{
    extern const Metric ColumnsCacheBytes;
    extern const Metric ColumnsCacheEntries;
}

using namespace DB;

namespace
{

ColumnsCache::MappedPtr makeEntry(size_t rows)
{
    auto column = ColumnUInt64::create();
    column->getData().resize_fill(rows, 0);
    return std::make_shared<ColumnsCacheEntry>(ColumnsCacheEntry{std::move(column), rows});
}

}

TEST(ColumnsCache, SetAndGetIntersecting)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100};
    EXPECT_TRUE(cache.set(key, makeEntry(100), 0));

    auto intersecting = cache.getIntersecting(table_uuid, "part_1", "col", 10, 20);
    ASSERT_EQ(intersecting.size(), 1);
    EXPECT_EQ(intersecting[0].first, key);

    /// A write fully covered by an existing wider interval is a no-op
    /// and must report that no bytes were written.
    ColumnsCacheKey narrow_key{table_uuid, "part_1", "col", 10, 20};
    EXPECT_FALSE(cache.set(narrow_key, makeEntry(10), 0));
}

TEST(ColumnsCache, OversizedEntryRejected)
{
    /// 10 rows of UInt64 plus the per-entry overhead fit into 1024 bytes,
    /// 1000 rows do not.
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1024, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey big_key{table_uuid, "part_1", "col", 0, 1000};
    EXPECT_FALSE(cache.set(big_key, makeEntry(1000), 0));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 1000).empty());
}

TEST(ColumnsCache, OversizedEntryDoesNotEraseOverlappingRanges)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1024, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey small_key{table_uuid, "part_1", "col", 0, 10};
    EXPECT_TRUE(cache.set(small_key, makeEntry(10), 0));

    /// A replacement that cannot stay resident must be rejected before it
    /// erases useful overlapping cached ranges.
    ColumnsCacheKey big_key{table_uuid, "part_1", "col", 0, 1000};
    EXPECT_FALSE(cache.set(big_key, makeEntry(1000), 0));

    auto intersecting = cache.getIntersecting(table_uuid, "part_1", "col", 0, 10);
    ASSERT_EQ(intersecting.size(), 1);
    EXPECT_EQ(intersecting[0].first, small_key);
    EXPECT_EQ(intersecting[0].second->rows, 10u);
}

TEST(ColumnsCache, SLRUOversizedEntryRejected)
{
    ColumnsCache cache("SLRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1024, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey big_key{table_uuid, "part_1", "col", 0, 1000};
    EXPECT_FALSE(cache.set(big_key, makeEntry(1000), 0));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 1000).empty());

    /// An entry within the size limit is admitted even when it is larger than
    /// the protected segment of the SLRU policy.
    ColumnsCacheKey medium_key{table_uuid, "part_1", "col", 0, 60};
    EXPECT_TRUE(cache.set(medium_key, makeEntry(60), 0));
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 60).size(), 1);
}
