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

TEST(ColumnsCache, SLRUFailedAdmissionPreservesOverlappingRanges)
{
    /// max_protected = size_ratio * max = 512 B. A 696 B entry is within the
    /// 1024 B limit (so the up-front weight check does not reject it), but once a
    /// 336 B entry occupies the protected segment, SLRU evicts the freshly
    /// inserted probationary entry on insertion (336 + 696 > 1024). This failed
    /// admission must not erase the overlapping range that is already cached, and
    /// must leave the cache and its side index in a consistent state.
    ColumnsCache cache("SLRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1024, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey key_a{table_uuid, "part_1", "col", 0, 10};
    ASSERT_TRUE(cache.set(key_a, makeEntry(10), 0));

    /// Promote A into the protected segment so the probationary overflow sweep
    /// triggered by the next insertion cannot evict it.
    ASSERT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 10).size(), 1u);

    ColumnsCacheKey key_b{table_uuid, "part_1", "col", 0, 55};
    auto entry_b = makeEntry(55);
    ASSERT_LE(ColumnsCacheWeightFunction{}(*entry_b), 1024u);
    EXPECT_FALSE(cache.set(key_b, entry_b, 0));

    /// A must still be served after B's failed admission.
    auto intersecting = cache.getIntersecting(table_uuid, "part_1", "col", 0, 10);
    ASSERT_EQ(intersecting.size(), 1u);
    EXPECT_EQ(intersecting[0].first, key_a);
    EXPECT_EQ(intersecting[0].second->rows, 10u);

    /// A subsequent in-limit write to a fresh part still succeeds, i.e. the failed
    /// admission did not leave a dangling side-index bucket or corrupt the cache.
    ColumnsCacheKey key_c{table_uuid, "part_2", "col", 0, 10};
    EXPECT_TRUE(cache.set(key_c, makeEntry(10), 0));
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_2", "col", 0, 10).size(), 1u);
}
