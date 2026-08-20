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
    const auto [table_generation, part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    EXPECT_TRUE(cache.set(key, makeEntry(100), table_generation, part_generation));

    auto intersecting = cache.getIntersecting(table_uuid, "part_1", "col", 10, 20);
    ASSERT_EQ(intersecting.size(), 1);
    EXPECT_EQ(intersecting[0].first, key);

    /// A write fully covered by an existing wider interval is a no-op
    /// and must report that no bytes were written.
    ColumnsCacheKey narrow_key{table_uuid, "part_1", "col", 10, 20};
    EXPECT_FALSE(cache.set(narrow_key, makeEntry(10), table_generation, part_generation));
}

TEST(ColumnsCache, ClearAllIsStickyAgainstInFlightReaders)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    /// A reader captures the generation when it starts reading.
    const auto [table_generation, part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");

    /// `SYSTEM DROP COLUMNS CACHE` happens while that reader is still running.
    cache.clearAll();

    /// The reader's deferred write must not resurrect entries after the drop.
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100};
    EXPECT_FALSE(cache.set(key, makeEntry(100), table_generation, part_generation));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100).empty());

    /// A reader that starts after the drop caches normally again.
    const auto [new_table_generation, new_part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    EXPECT_TRUE(cache.set(key, makeEntry(100), new_table_generation, new_part_generation));
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100).size(), 1u);
}

TEST(ColumnsCache, ClearAllAndRemoveTableGenerationsDoNotCancelOut)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    const auto [table_generation, part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");

    /// Both kinds of invalidation happen while a reader is in flight. The token is
    /// a sum of two monotonically increasing counters, so no combination of bumps
    /// can bring it back to a previously observed value.
    cache.clearAll();
    cache.removeTable(table_uuid);
    EXPECT_NE(cache.getInvalidationGenerations(table_uuid, "part_1").first, table_generation);

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100};
    EXPECT_FALSE(cache.set(key, makeEntry(100), table_generation, part_generation));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100).empty());
}

TEST(ColumnsCache, OversizedEntryRejected)
{
    /// 10 rows of UInt64 plus the per-entry overhead fit into 1024 bytes,
    /// 1000 rows do not.
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1024, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey big_key{table_uuid, "part_1", "col", 0, 1000};
    EXPECT_FALSE(cache.set(big_key, makeEntry(1000), 0, 0));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 1000).empty());
}

TEST(ColumnsCache, OversizedEntryDoesNotEraseOverlappingRanges)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1024, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey small_key{table_uuid, "part_1", "col", 0, 10};
    EXPECT_TRUE(cache.set(small_key, makeEntry(10), 0, 0));

    /// A replacement that cannot stay resident must be rejected before it
    /// erases useful overlapping cached ranges.
    ColumnsCacheKey big_key{table_uuid, "part_1", "col", 0, 1000};
    EXPECT_FALSE(cache.set(big_key, makeEntry(1000), 0, 0));

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
    EXPECT_FALSE(cache.set(big_key, makeEntry(1000), 0, 0));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 1000).empty());

    /// An entry within the size limit is admitted even when it is larger than
    /// the protected segment of the SLRU policy.
    ColumnsCacheKey medium_key{table_uuid, "part_1", "col", 0, 60};
    EXPECT_TRUE(cache.set(medium_key, makeEntry(60), 0, 0));
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
    ASSERT_TRUE(cache.set(key_a, makeEntry(10), 0, 0));

    /// Promote A into the protected segment so the probationary overflow sweep
    /// triggered by the next insertion cannot evict it.
    ASSERT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 10).size(), 1u);

    ColumnsCacheKey key_b{table_uuid, "part_1", "col", 0, 55};
    auto entry_b = makeEntry(55);
    ASSERT_LE(ColumnsCacheWeightFunction{}(*entry_b), 1024u);
    EXPECT_FALSE(cache.set(key_b, entry_b, 0, 0));

    /// A must still be served after B's failed admission.
    auto intersecting = cache.getIntersecting(table_uuid, "part_1", "col", 0, 10);
    ASSERT_EQ(intersecting.size(), 1u);
    EXPECT_EQ(intersecting[0].first, key_a);
    EXPECT_EQ(intersecting[0].second->rows, 10u);

    /// A subsequent in-limit write to a fresh part still succeeds, i.e. the failed
    /// admission did not leave a dangling side-index bucket or corrupt the cache.
    ColumnsCacheKey key_c{table_uuid, "part_2", "col", 0, 10};
    EXPECT_TRUE(cache.set(key_c, makeEntry(10), 0, 0));
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_2", "col", 0, 10).size(), 1u);
}

TEST(ColumnsCache, RemovePartIsStickyAgainstInFlightReaders)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100};

    /// A reader captures both generations before the part is removed.
    const auto [table_generation, part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    cache.removePart(table_uuid, "part_1");

    /// Its deferred write cannot resurrect the removed part.
    EXPECT_FALSE(cache.set(key, makeEntry(100), table_generation, part_generation));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100).empty());

    /// A reader started after removal can populate a newly attached part with
    /// that name normally.
    const auto [new_table_generation, new_part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    EXPECT_TRUE(cache.set(key, makeEntry(100), new_table_generation, new_part_generation));
}

TEST(ColumnsCache, ClearAllRejectsTokensOfEveryEarlierInvalidation)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    /// `clearAll` forgets the per-table stamps, so its own stamp must be greater than every
    /// table stamp handed out before it, no matter how many times the table was invalidated.
    cache.removeTable(table_uuid);
    const auto [stale_table_generation, stale_part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    cache.removeTable(table_uuid);
    cache.clearAll();

    EXPECT_NE(cache.getInvalidationGenerations(table_uuid, "part_1").first, stale_table_generation);

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100, stale_table_generation};
    EXPECT_FALSE(cache.set(key, makeEntry(100), stale_table_generation, stale_part_generation));
}

TEST(ColumnsCache, EntriesAreNotVisibleAcrossTableGenerations)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    const auto [table_generation, part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100, table_generation};
    EXPECT_TRUE(cache.set(key, makeEntry(100), table_generation, part_generation));

    /// The same reader repeating the read finds its entry.
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100, table_generation).size(), 1u);
    /// A reader that observed a later invalidation of the table does not.
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100, table_generation + 1).empty());
}

TEST(ColumnsCache, DisabledCacheDoesNotRememberInvalidations)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 0, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    /// A cache that admits nothing must not accumulate invalidation metadata for every table
    /// and part of the server.
    cache.removeTable(table_uuid);
    cache.removePart(table_uuid, "part_1");
    EXPECT_EQ(cache.getInvalidationGenerations(table_uuid, "part_1"), std::make_pair(UInt64(0), UInt64(0)));
}

TEST(ColumnsCache, RemoveTableReclaimsPartGenerationTombstones)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    const auto [initial_table_generation, initial_part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    cache.removePart(table_uuid, "part_1");
    EXPECT_NE(cache.getInvalidationGenerations(table_uuid, "part_1").second, initial_part_generation);

    /// Dropping a table invalidates in-flight readers through the table token,
    /// so the per-part tombstone is no longer needed.
    cache.removeTable(table_uuid);
    const auto [table_generation, part_generation] = cache.getInvalidationGenerations(table_uuid, "part_1");
    EXPECT_NE(table_generation, initial_table_generation);
    EXPECT_EQ(part_generation, 0u);
}
