#include <gtest/gtest.h>

#include <algorithm>

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
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);
    EXPECT_TRUE(cache.set(key, makeEntry(100), table_generation));

    auto intersecting = cache.getIntersecting(table_uuid, "part_1", "col", 10, 20);
    ASSERT_EQ(intersecting.size(), 1);
    EXPECT_EQ(intersecting[0].first, key);

    /// A write fully covered by an existing wider interval is a no-op
    /// and must report that no bytes were written.
    ColumnsCacheKey narrow_key{table_uuid, "part_1", "col", 10, 20};
    EXPECT_FALSE(cache.set(narrow_key, makeEntry(10), table_generation));
}

TEST(ColumnsCache, ClearAllIsStickyAgainstInFlightReaders)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    /// A reader captures the generation when it starts reading.
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    /// `SYSTEM DROP COLUMNS CACHE` happens while that reader is still running.
    cache.clearAll();

    /// The reader's deferred write must not resurrect entries after the drop.
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100};
    EXPECT_FALSE(cache.set(key, makeEntry(100), table_generation));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100).empty());

    /// A reader that starts after the drop caches normally again.
    const auto new_table_generation = cache.getInvalidationGeneration(table_uuid);
    EXPECT_TRUE(cache.set(key, makeEntry(100), new_table_generation));
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100).size(), 1u);
}

TEST(ColumnsCache, ClearAllAndRemoveTableGenerationsDoNotCancelOut)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    /// Both kinds of invalidation happen while a reader is in flight. Every stamp comes from
    /// a single monotonically increasing counter and the token is the later of the two, so no
    /// combination of bumps can bring it back to a previously observed value.
    cache.clearAll();
    cache.removeTable(table_uuid);
    EXPECT_NE(cache.getInvalidationGeneration(table_uuid), table_generation);

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100};
    EXPECT_FALSE(cache.set(key, makeEntry(100), table_generation));
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
    /// The limit is derived from the weight function instead of being written out in bytes, so
    /// that a change in what an entry is charged for - the retained memory rather than the size
    /// of its rows, for instance - cannot silently turn the admitted case into a rejected one.
    auto medium_entry = makeEntry(60);
    const size_t medium_weight = ColumnsCacheWeightFunction{}(*medium_entry);
    /// Larger than the protected segment (`size_ratio` * max), smaller than the whole cache.
    const size_t max_size_in_bytes = medium_weight + medium_weight / 2;

    ColumnsCache cache("SLRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        max_size_in_bytes, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey big_key{table_uuid, "part_1", "col", 0, 1000};
    auto big_entry = makeEntry(1000);
    ASSERT_GT(ColumnsCacheWeightFunction{}(*big_entry), max_size_in_bytes);
    EXPECT_FALSE(cache.set(big_key, big_entry, 0));
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 1000).empty());

    /// An entry within the size limit is admitted even when it is larger than
    /// the protected segment of the SLRU policy.
    ColumnsCacheKey medium_key{table_uuid, "part_1", "col", 0, 60};
    EXPECT_TRUE(cache.set(medium_key, medium_entry, 0));
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 60).size(), 1);
}

TEST(ColumnsCache, SLRUFailedAdmissionPreservesOverlappingRanges)
{
    /// B is within the overall size limit, so the up-front weight check does not reject it, but
    /// once A occupies the protected segment SLRU evicts the freshly inserted probationary B on
    /// insertion, because A and B together do not fit. This failed admission must not erase the
    /// overlapping range that is already cached, and must leave the cache and its side index in
    /// a consistent state.
    ///
    /// The limit is computed from the two entries rather than written out in bytes, so that the
    /// three properties the test needs - A fits into the protected segment, B fits into the
    /// cache, and A and B together do not - hold whatever an entry is charged for.
    auto entry_a = makeEntry(10);
    auto entry_b = makeEntry(1000);
    const size_t weight_a = ColumnsCacheWeightFunction{}(*entry_a);
    const size_t weight_b = ColumnsCacheWeightFunction{}(*entry_b);
    ASSERT_LT(weight_a, weight_b);
    const size_t max_size_in_bytes = std::max(2 * weight_a, weight_b);
    ASSERT_LT(max_size_in_bytes, weight_a + weight_b);

    ColumnsCache cache("SLRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        max_size_in_bytes, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    ColumnsCacheKey key_a{table_uuid, "part_1", "col", 0, 10};
    ASSERT_TRUE(cache.set(key_a, entry_a, 0));

    /// Promote A into the protected segment so the probationary overflow sweep
    /// triggered by the next insertion cannot evict it.
    ASSERT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 10).size(), 1u);

    ColumnsCacheKey key_b{table_uuid, "part_1", "col", 0, 1000};
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

TEST(ColumnsCache, ClearAllRejectsTokensOfEveryEarlierInvalidation)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    /// `clearAll` forgets the per-table stamps, so its own stamp must be greater than every
    /// table stamp handed out before it, no matter how many times the table was invalidated.
    cache.removeTable(table_uuid);
    const auto stale_table_generation = cache.getInvalidationGeneration(table_uuid);
    cache.removeTable(table_uuid);
    cache.clearAll();

    EXPECT_NE(cache.getInvalidationGeneration(table_uuid), stale_table_generation);

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100, stale_table_generation};
    EXPECT_FALSE(cache.set(key, makeEntry(100), stale_table_generation));
}

TEST(ColumnsCache, EntriesAreNotVisibleAcrossTableGenerations)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    const auto table_generation = cache.getInvalidationGeneration(table_uuid);
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100, table_generation};
    EXPECT_TRUE(cache.set(key, makeEntry(100), table_generation));

    /// The same reader repeating the read finds its entry.
    EXPECT_EQ(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100, table_generation).size(), 1u);
    /// A reader that observed a later invalidation of the table does not.
    EXPECT_TRUE(cache.getIntersecting(table_uuid, "part_1", "col", 0, 100, table_generation + 1).empty());
}

TEST(ColumnsCache, DisabledCacheStillInvalidatesInFlightReaders)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    /// A reader that started while the cache was enabled holds a token.
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    /// The cache is disabled by a config reload, the table is altered, and the cache is
    /// enabled again before the reader gets to its deferred write. The invalidation happened
    /// while the cache was disabled, but it still has to reject that write.
    cache.setMaxSizeInBytesAndCompact(0);
    cache.removeTable(table_uuid);
    cache.setMaxSizeInBytesAndCompact(1 << 20);

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, 100, 0};
    EXPECT_FALSE(cache.set(key, makeEntry(100), table_generation));

    /// A reader that starts after all of it can write again.
    const auto fresh_table_generation = cache.getInvalidationGeneration(table_uuid);
    EXPECT_TRUE(cache.set(key, makeEntry(100), fresh_table_generation));
}

TEST(ColumnsCache, DisabledCacheDoesNotAccumulatePerTableInvalidationMetadata)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 0, /*max_count=*/ 0, /*size_ratio=*/ 0.5);

    /// A cache that admits nothing must not accumulate invalidation metadata for every table
    /// of the server: the invalidations are folded into the single cache-wide stamp,
    /// so every table observes the same one and no per-table entry is kept.
    const UUID first_table = UUIDHelpers::generateV4();
    const UUID second_table = UUIDHelpers::generateV4();
    cache.removeTable(first_table);
    cache.removeTable(second_table);

    EXPECT_EQ(cache.getInvalidationGeneration(first_table), cache.getInvalidationGeneration(second_table));
}

