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

ColumnsCache::MappedPtr makeEntry(const ColumnsCacheKey & key, size_t rows)
{
    auto column = ColumnUInt64::create();
    column->getData().resize_fill(rows, 0);
    auto entry = std::make_shared<ColumnsCacheEntry>();
    entry->column = std::move(column);
    entry->row_begin = key.mark * rows;
    entry->rows = rows;
    entry->key = key;
    return entry;
}

size_t countPresent(ColumnsCache & cache, const UUID & table_uuid, const String & part, const String & column, size_t first_mark, size_t end_mark, UInt64 schema_identity = 0)
{
    auto entries = cache.getMany(table_uuid, part, column, schema_identity, first_mark, end_mark);
    return std::count_if(entries.begin(), entries.end(), [](const auto & e) { return e != nullptr; });
}

}

TEST(ColumnsCache, SetAndGetMany)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    /// Granules 0, 1 and 3 of the column are written; 2 is not.
    for (size_t mark : {0, 1, 3})
        EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", mark}, 100), table_generation));

    auto entries = cache.getMany(table_uuid, "part_1", "col", 0, 0, 5);
    ASSERT_EQ(entries.size(), 5u);
    EXPECT_TRUE(entries[0] && entries[1] && entries[3]);
    EXPECT_FALSE(entries[2] || entries[4]);
    EXPECT_EQ(entries[3]->key.mark, 3u);

    /// Writing a granule again replaces the entry and keeps the count.
    EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", 1}, 100), table_generation));
    EXPECT_EQ(cache.count(), 3u);
}

TEST(ColumnsCache, SetManyChargesOnlyAdmittedEntries)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    std::vector<ColumnsCache::MappedPtr> entries;
    for (size_t mark = 0; mark < 4; ++mark)
        entries.push_back(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", mark}, 100));
    /// An entry that cannot fit into the cache at all is not charged.
    entries.push_back(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", 4}, 1 << 20));

    size_t expected_bytes = 0;
    for (size_t i = 0; i < 4; ++i)
        expected_bytes += ColumnsCacheWeightFunction{}(*entries[i]);

    EXPECT_EQ(cache.setMany(entries, table_generation), expected_bytes);
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 5), 4u);

    /// A stale generation admits nothing.
    cache.removeTable(table_uuid);
    EXPECT_EQ(cache.setMany(entries, table_generation), 0u);
    EXPECT_EQ(cache.count(), 0u);
}

TEST(ColumnsCache, RemovePartDropsOnlyItsEntries)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    for (size_t mark = 0; mark < 3; ++mark)
    {
        EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "a", mark}, 10), table_generation));
        EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "b", mark}, 10), table_generation));
        EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_2", "a", mark}, 10), table_generation));
    }
    ASSERT_EQ(cache.count(), 9u);

    cache.removePart(table_uuid, "part_1");
    EXPECT_EQ(cache.count(), 3u);
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "a", 0, 3), 0u);
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "b", 0, 3), 0u);
    EXPECT_EQ(countPresent(cache, table_uuid, "part_2", "a", 0, 3), 3u);

    /// The stamp of the table is not advanced by removing a part: a reader that started before
    /// still writes.
    EXPECT_EQ(cache.getInvalidationGeneration(table_uuid), table_generation);
}

TEST(ColumnsCache, EvictedEntriesLeaveNoIndexBehind)
{
    /// Room for about two entries of 100 rows.
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 3000, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    for (size_t mark = 0; mark < 10; ++mark)
        cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", mark}, 100), table_generation);

    const size_t resident = cache.count();
    EXPECT_GT(resident, 0u);
    EXPECT_LT(resident, 10u);
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 10), resident);

    /// Removing the part removes exactly what is left and nothing breaks on the evicted keys.
    cache.removePart(table_uuid, "part_1");
    EXPECT_EQ(cache.count(), 0u);
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
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0};
    EXPECT_FALSE(cache.set(makeEntry(key, 100), table_generation));
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1), 0u);

    /// A reader that starts after the drop caches normally again.
    const auto new_table_generation = cache.getInvalidationGeneration(table_uuid);
    EXPECT_TRUE(cache.set(makeEntry(key, 100), new_table_generation));
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1), 1u);
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

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0};
    EXPECT_FALSE(cache.set(makeEntry(key, 100), table_generation));
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1), 0u);
}

TEST(ColumnsCache, OversizedEntryRejected)
{
    /// 10 rows of UInt64 plus the per-entry overhead fit into 2048 bytes,
    /// 1000 rows do not.
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 2048, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    EXPECT_FALSE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", 0}, 1000), 0));
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1), 0u);

    EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", 1}, 10), 0));
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 2), 1u);
}

TEST(ColumnsCache, RemoveTableInvalidatesEntriesAndInFlightWrites)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();
    const UUID other_table_uuid = UUIDHelpers::generateV4();

    const auto table_generation = cache.getInvalidationGeneration(table_uuid);
    const auto other_generation = cache.getInvalidationGeneration(other_table_uuid);
    EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", 0}, 10), table_generation));
    EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{other_table_uuid, "part_1", "col", 0}, 10), other_generation));

    cache.removeTable(table_uuid);

    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1), 0u);
    EXPECT_EQ(countPresent(cache, other_table_uuid, "part_1", "col", 0, 1), 1u);

    /// The deferred write of a reader that started before the invalidation is dropped.
    EXPECT_FALSE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", 1}, 10), table_generation));
    /// The other table is not affected.
    EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{other_table_uuid, "part_1", "col", 1}, 10), other_generation));
}

TEST(ColumnsCache, ClearAllStampExceedsEveryTableStamp)
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

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, stale_table_generation};
    EXPECT_FALSE(cache.set(makeEntry(key, 100), stale_table_generation));
}

TEST(ColumnsCache, EntriesAreNotVisibleAcrossSchemaIdentities)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    const UUID table_uuid = UUIDHelpers::generateV4();

    const auto table_generation = cache.getInvalidationGeneration(table_uuid);
    ColumnsCacheKey key{table_uuid, "part_1", "col", 0, /*schema_identity=*/ 7};
    EXPECT_TRUE(cache.set(makeEntry(key, 100), table_generation));

    /// The same reader repeating the read finds its entry.
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1, 7), 1u);
    /// A reader with another schema identity does not.
    EXPECT_EQ(countPresent(cache, table_uuid, "part_1", "col", 0, 1, 8), 0u);
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
    cache.setConfiguredMaxSizeInBytes(0);
    cache.removeTable(table_uuid);
    cache.setConfiguredMaxSizeInBytes(1 << 20);

    ColumnsCacheKey key{table_uuid, "part_1", "col", 0};
    EXPECT_FALSE(cache.set(makeEntry(key, 100), table_generation));

    /// A reader that starts after all of it can write again.
    const auto fresh_table_generation = cache.getInvalidationGeneration(table_uuid);
    EXPECT_TRUE(cache.set(makeEntry(key, 100), fresh_table_generation));
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

TEST(ColumnsCache, AutoResizeYieldsMemoryAndGrowsBack)
{
    ColumnsCache cache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries,
        /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
    cache.setAutoResizeSettings(/*free_memory_ratio=*/ 0.0, /*history_window_ms=*/ 0);
    const UUID table_uuid = UUIDHelpers::generateV4();
    const auto table_generation = cache.getInvalidationGeneration(table_uuid);

    for (size_t mark = 0; mark < 100; ++mark)
        EXPECT_TRUE(cache.set(makeEntry(ColumnsCacheKey{table_uuid, "part_1", "col", mark}, 100), table_generation));
    const size_t full_size = cache.sizeInBytes();
    ASSERT_EQ(cache.count(), 100u);

    /// The rest of the server uses all but a quarter of the limit: the cache shrinks to that
    /// quarter, whatever its configured size, and reports that the usage then fits.
    const size_t limit = 4 * full_size;
    const size_t others = 3 * full_size;
    EXPECT_TRUE(cache.autoResize(static_cast<Int64>(others + full_size), limit));
    EXPECT_EQ(cache.maxSizeInBytes(), full_size);
    EXPECT_EQ(cache.count(), 100u);

    /// The usage of the rest grows past what the limit leaves: entries are evicted.
    EXPECT_TRUE(cache.autoResize(static_cast<Int64>(limit - full_size / 2 + cache.sizeInBytes()), limit));
    EXPECT_LE(cache.sizeInBytes(), full_size / 2);
    EXPECT_LT(cache.count(), 100u);
    EXPECT_GT(cache.count(), 0u);

    /// Nothing else uses memory any more: the cache may grow back, but not beyond its
    /// configured size.
    EXPECT_TRUE(cache.autoResize(static_cast<Int64>(cache.sizeInBytes()), 100 << 20));
    EXPECT_EQ(cache.maxSizeInBytes(), 1u << 20);

    /// The usage of the rest exceeds the limit by itself: the cache gives up everything and
    /// reports that it is not enough.
    EXPECT_FALSE(cache.autoResize(static_cast<Int64>(2 * limit), limit));
    EXPECT_EQ(cache.count(), 0u);
}
