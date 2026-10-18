#include <gtest/gtest.h>

#include <algorithm>

#include <Columns/ColumnSparse.h>
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

constexpr size_t ROWS_PER_MARK = 100;

struct TestColumn
{
    UUID table_uuid;
    String part;
    String column;
    UInt128 identity;

    TestColumn(const UUID & table_uuid_, const String & part_, const String & column_, UInt64 schema_identity = 0)
        : table_uuid(table_uuid_), part(part_), column(column_)
        , identity(getColumnsCacheColumnIdentity(table_uuid_, part_, column_, schema_identity))
    {
    }
};

/// An entry of the granules [first_mark, end_mark) of a stripe of 8 granules, filled with the mark numbers.
ColumnsCache::MappedPtr makeEntry(const TestColumn & c, size_t stripe, size_t first_mark, size_t end_mark)
{
    auto column = ColumnUInt64::create();
    for (size_t mark = first_mark; mark < end_mark; ++mark)
        column->getData().resize_fill(column->size() + ROWS_PER_MARK, mark);

    auto entry = std::make_shared<ColumnsCacheEntry>();
    entry->column = std::move(column);
    entry->table_uuid = c.table_uuid;
    entry->part_name = c.part;
    entry->column_name = c.column;
    entry->first_mark = first_mark;
    entry->end_mark = end_mark;
    entry->row_begin = first_mark * ROWS_PER_MARK;
    entry->rows = (end_mark - first_mark) * ROWS_PER_MARK;
    entry->key = ColumnsCacheKey{c.identity, stripe};
    return entry;
}

/// The same entry with its rows in a `ColumnSparse`, as a read of a sparsely serialized column
/// of the part produces. Mark 0 is filled with zeroes, so the sparse form is not degenerate.
ColumnsCache::MappedPtr makeSparseEntry(const TestColumn & c, size_t stripe, size_t first_mark, size_t end_mark)
{
    auto entry = makeEntry(c, stripe, first_mark, end_mark);
    auto sparse = ColumnSparse::create(entry->column->cloneEmpty());
    sparse->insertRangeFrom(*entry->column, 0, entry->column->size());
    entry->column = std::move(sparse);
    return entry;
}

ColumnsCache::MappedPtr getOne(ColumnsCache & cache, const TestColumn & c, size_t stripe)
{
    auto entries = cache.getMany(c.identity, stripe, stripe + 1);
    return entries.at(0);
}

size_t countPresent(ColumnsCache & cache, const TestColumn & c, size_t first_stripe, size_t end_stripe)
{
    auto entries = cache.getMany(c.identity, first_stripe, end_stripe);
    return std::count_if(entries.begin(), entries.end(), [](const auto & e) { return e != nullptr; });
}

ColumnsCache makeCache(size_t max_size_in_bytes = 1 << 24)
{
    return ColumnsCache("LRU", CurrentMetrics::ColumnsCacheBytes, CurrentMetrics::ColumnsCacheEntries, max_size_in_bytes, /*max_count=*/ 0, /*size_ratio=*/ 0.5);
}

}

TEST(ColumnsCache, Stripes)
{
    /// 100 rows per mark: a stripe is 655 marks; clamped to 256.
    EXPECT_EQ(ColumnsCacheStripes(100).stripe_marks, 256u);
    /// 8192 rows per mark: 8 marks per stripe.
    EXPECT_EQ(ColumnsCacheStripes(8192).stripe_marks, 8u);
    /// Huge granules: one mark per stripe.
    EXPECT_EQ(ColumnsCacheStripes(500000).stripe_marks, 1u);

    ColumnsCacheStripes stripes(8192);
    EXPECT_EQ(stripes.stripeOf(0), 0u);
    EXPECT_EQ(stripes.stripeOf(7), 0u);
    EXPECT_EQ(stripes.stripeOf(8), 1u);
    EXPECT_EQ(stripes.firstMark(3), 24u);
    EXPECT_EQ(stripes.endMark(12, 100), 100u);
}

TEST(ColumnsCache, SetAndGetMany)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    /// Stripes 0, 1 and 3 are written; 2 is not.
    std::vector<ColumnsCache::MappedPtr> entries;
    for (size_t stripe : {0, 1, 3})
        entries.push_back(makeEntry(c, stripe, stripe * 8, stripe * 8 + 8));
    EXPECT_GT(cache.setMany(entries, generation), 0u);

    auto found = cache.getMany(c.identity, 0, 5);
    ASSERT_EQ(found.size(), 5u);
    EXPECT_TRUE(found[0] && found[1] && found[3]);
    EXPECT_FALSE(found[2] || found[4]);
    EXPECT_EQ(found[3]->key.stripe, 3u);
    EXPECT_EQ(cache.count(), 3u);

    /// Another column of the same part does not find them.
    TestColumn other(c.table_uuid, "part_1", "other");
    EXPECT_EQ(countPresent(cache, other, 0, 5), 0u);
}

TEST(ColumnsCache, PartialStripesAreMerged)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    /// The first read touched granules [2, 5) of stripe 0.
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 2, 5)}, generation), 0u);
    auto entry = getOne(cache, c, 0);
    ASSERT_TRUE(entry);
    EXPECT_TRUE(entry->coversMarks(2, 5));
    EXPECT_FALSE(entry->coversMarks(1, 5));

    /// A read within it adds nothing and is not charged.
    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 3, 4)}, generation), 0u);
    EXPECT_EQ(cache.count(), 1u);

    /// An adjacent read on the right extends it.
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 5, 8)}, generation), 0u);
    entry = getOne(cache, c, 0);
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry->first_mark, 2u);
    EXPECT_EQ(entry->end_mark, 8u);
    EXPECT_EQ(entry->rows, 6 * ROWS_PER_MARK);

    /// An overlapping read on the left extends it too, and the rows come out in order.
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 0, 3)}, generation), 0u);
    entry = getOne(cache, c, 0);
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry->first_mark, 0u);
    EXPECT_EQ(entry->end_mark, 8u);
    ASSERT_EQ(entry->column->size(), 8 * ROWS_PER_MARK);
    for (size_t mark = 0; mark < 8; ++mark)
        EXPECT_EQ(entry->column->getUInt(mark * ROWS_PER_MARK + 1), mark);
    EXPECT_EQ(cache.count(), 1u);
}

TEST(ColumnsCache, DisjointPartialStripesKeepTheLarger)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    EXPECT_GT(cache.setMany({makeEntry(c, 0, 0, 3)}, generation), 0u);
    /// Smaller and disjoint: dropped.
    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 6, 8)}, generation), 0u);
    EXPECT_TRUE(getOne(cache, c, 0)->coversMarks(0, 3));
    /// Larger and disjoint: replaces.
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 4, 8)}, generation), 0u);
    EXPECT_TRUE(getOne(cache, c, 0)->coversMarks(4, 8));
    EXPECT_FALSE(getOne(cache, c, 0)->coversMarks(0, 3));
}

TEST(ColumnsCache, RemovePartDropsOnlyItsEntries)
{
    auto cache = makeCache();
    const UUID table_uuid = UUIDHelpers::generateV4();
    TestColumn a1(table_uuid, "part_1", "a");
    TestColumn b1(table_uuid, "part_1", "b");
    TestColumn a2(table_uuid, "part_2", "a");
    const auto generation = cache.getInvalidationGeneration(table_uuid);

    for (size_t stripe = 0; stripe < 3; ++stripe)
        for (const auto * c : {&a1, &b1, &a2})
            EXPECT_GT(cache.setMany({makeEntry(*c, stripe, stripe * 8, stripe * 8 + 8)}, generation), 0u);
    ASSERT_EQ(cache.count(), 9u);

    cache.removePart(table_uuid, "part_1");
    EXPECT_EQ(cache.count(), 3u);
    EXPECT_EQ(countPresent(cache, a1, 0, 3), 0u);
    EXPECT_EQ(countPresent(cache, b1, 0, 3), 0u);
    EXPECT_EQ(countPresent(cache, a2, 0, 3), 3u);

    /// The stamp of the table is not advanced by removing a part: a reader that started before
    /// still writes.
    EXPECT_EQ(cache.getInvalidationGeneration(table_uuid), generation);
}

TEST(ColumnsCache, EvictedEntriesLeaveNoIndexBehind)
{
    /// Room for a few entries of 800 rows of UInt64 in every shard.
    auto cache = makeCache(ColumnsCache::numberOfShards(20000) * 20000);
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    for (size_t stripe = 0; stripe < 100; ++stripe)
        cache.setMany({makeEntry(c, stripe, stripe * 8, stripe * 8 + 8)}, generation);

    const size_t resident = cache.count();
    EXPECT_GT(resident, 0u);
    EXPECT_LT(resident, 100u);
    EXPECT_EQ(countPresent(cache, c, 0, 100), resident);

    /// Removing the part removes exactly what is left and nothing breaks on the evicted keys.
    cache.removePart(c.table_uuid, "part_1");
    EXPECT_EQ(cache.count(), 0u);
}

TEST(ColumnsCache, ClearAllIsStickyAgainstInFlightReaders)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");

    /// A reader captures the generation when it starts reading.
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    /// `SYSTEM DROP COLUMNS CACHE` happens while that reader is still running.
    cache.clearAll();

    /// The reader's deferred write must not resurrect entries after the drop.
    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);
    EXPECT_EQ(countPresent(cache, c, 0, 1), 0u);

    /// A reader that starts after the drop caches normally again.
    const auto new_generation = cache.getInvalidationGeneration(c.table_uuid);
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 0, 8)}, new_generation), 0u);
    EXPECT_EQ(countPresent(cache, c, 0, 1), 1u);
}

TEST(ColumnsCache, ClearAllAndRemoveTableGenerationsDoNotCancelOut)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    /// Both kinds of invalidation happen while a reader is in flight. Every stamp comes from
    /// a single monotonically increasing counter and the token is the later of the two, so no
    /// combination of bumps can bring it back to a previously observed value.
    cache.clearAll();
    cache.removeTable(c.table_uuid);
    EXPECT_NE(cache.getInvalidationGeneration(c.table_uuid), generation);

    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);
    EXPECT_EQ(countPresent(cache, c, 0, 1), 0u);
}

TEST(ColumnsCache, OversizedEntryRejected)
{
    /// Every shard holds 2 KiB: one granule of 100 rows of UInt64 fits, eight do not.
    auto cache = makeCache(ColumnsCache::numberOfShards(2048) * 2048);
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);
    EXPECT_EQ(countPresent(cache, c, 0, 1), 0u);

    EXPECT_GT(cache.setMany({makeEntry(c, 1, 8, 9)}, generation), 0u);
    EXPECT_EQ(countPresent(cache, c, 0, 2), 1u);
}

TEST(ColumnsCache, RemoveTableInvalidatesEntriesAndInFlightWrites)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    TestColumn other(UUIDHelpers::generateV4(), "part_1", "col");

    const auto generation = cache.getInvalidationGeneration(c.table_uuid);
    const auto other_generation = cache.getInvalidationGeneration(other.table_uuid);
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);
    EXPECT_GT(cache.setMany({makeEntry(other, 0, 0, 8)}, other_generation), 0u);

    cache.removeTable(c.table_uuid);

    EXPECT_EQ(countPresent(cache, c, 0, 1), 0u);
    EXPECT_EQ(countPresent(cache, other, 0, 1), 1u);

    /// The deferred write of a reader that started before the invalidation is dropped.
    EXPECT_EQ(cache.setMany({makeEntry(c, 1, 8, 16)}, generation), 0u);
    /// The other table is not affected.
    EXPECT_GT(cache.setMany({makeEntry(other, 1, 8, 16)}, other_generation), 0u);
}

TEST(ColumnsCache, InvalidationLandingMidInsertRemovesTheStaleWrite)
{
    /// The invalidation tests above all invalidate before `setMany` runs, so the write is rejected
    /// by the first generation check under `index_mutex` and they say nothing about the recheck
    /// after the insertion. Here the invalidation lands in exactly the window that recheck exists
    /// for: the entries have passed the first check and are recorded in the index, and are not in
    /// the shards yet.
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    size_t staged = 0;
    cache.on_entries_staged_for_test = [&]
    {
        ++staged;
        cache.removeTable(c.table_uuid);
    };

    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8), makeEntry(c, 1, 8, 16)}, generation), 0u);
    EXPECT_EQ(staged, 1u);
    cache.on_entries_staged_for_test = nullptr;

    /// Neither entry stays resident, and nothing of the part is left in the index.
    EXPECT_EQ(countPresent(cache, c, 0, 2), 0u);
    EXPECT_EQ(cache.count(), 0u);
    EXPECT_FALSE(cache.containsPart(c.table_uuid, "part_1"));

    /// A reader that starts after the invalidation writes normally again.
    const auto new_generation = cache.getInvalidationGeneration(c.table_uuid);
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 0, 8)}, new_generation), 0u);
    EXPECT_EQ(countPresent(cache, c, 0, 1), 1u);
}

TEST(ColumnsCache, ClearAllLandingMidInsertRemovesTheStaleWrite)
{
    /// The same window, invalidated by `SYSTEM DROP COLUMNS CACHE` instead of a metadata change:
    /// `clearAll` clears the shards before these entries reach them, so only the recheck can.
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    cache.on_entries_staged_for_test = [&] { cache.clearAll(); };
    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);
    cache.on_entries_staged_for_test = nullptr;

    EXPECT_EQ(countPresent(cache, c, 0, 1), 0u);
    EXPECT_EQ(cache.count(), 0u);
    EXPECT_FALSE(cache.containsPart(c.table_uuid, "part_1"));
}

TEST(ColumnsCache, StaleWriteLeavesAFreshWriteOfTheSameKeyAlone)
{
    /// The same window again, but with a reader that starts after the drop and writes the very
    /// same key while the stale write is still on its way out. That reader is exactly the one
    /// that should repopulate the cache after `SYSTEM DROP COLUMNS CACHE`, so the stale cleanup
    /// has to take out the entries it inserted itself and not everything under their keys.
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    ColumnsCache::MappedPtr fresh;
    bool fired = false;
    cache.on_entries_inserted_for_test = [&]
    {
        /// The write of the post-drop reader is an ordinary `setMany`; it must not re-enter here.
        /// A flag and not resetting the hook: assigning to the `std::function` from inside its own
        /// call destroys the closure whose captures the rest of this body still uses.
        if (fired)
            return;
        fired = true;

        cache.clearAll();
        fresh = makeEntry(c, 0, 0, 8);
        EXPECT_GT(cache.setMany({fresh}, cache.getInvalidationGeneration(c.table_uuid)), 0u);
    };

    /// The stale write is rejected and charges nothing.
    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);
    EXPECT_TRUE(fired);
    cache.on_entries_inserted_for_test = nullptr;

    /// The fresh entry is still there, and the index still knows about it.
    ASSERT_TRUE(fresh);
    EXPECT_EQ(cache.count(), 1u);
    EXPECT_EQ(getOne(cache, c, 0), fresh);
    EXPECT_TRUE(cache.containsPart(c.table_uuid, "part_1"));

    /// And the index entry is the real one: removing the part takes the fresh entry with it.
    cache.removePart(c.table_uuid, "part_1");
    EXPECT_EQ(cache.count(), 0u);
    EXPECT_FALSE(cache.containsPart(c.table_uuid, "part_1"));
}

TEST(ColumnsCache, AdjacentRunsOfDifferentRepresentationsMerge)
{
    /// The copy a read accumulates for the cache is a clone of the column the read produced, so
    /// the same column of the same part reaches the cache as a `ColumnSparse` under one set of
    /// settings and as a full column under another - the case
    /// `MergeTreeReaderWide::serveRowsFromColumnsCache` handles when it serves rows. Two adjacent
    /// runs of a stripe have to merge across that difference: a full column cannot take rows
    /// from a `ColumnSparse`.
    auto cache = makeCache();
    const auto expect_whole_stripe = [](const ColumnsCache::MappedPtr & entry)
    {
        ASSERT_TRUE(entry);
        EXPECT_EQ(entry->first_mark, 0u);
        EXPECT_EQ(entry->end_mark, 8u);
        ASSERT_EQ(entry->column->size(), 8 * ROWS_PER_MARK);
        for (size_t mark = 0; mark < 8; ++mark)
            EXPECT_EQ(entry->column->getUInt(mark * ROWS_PER_MARK + 1), mark);
    };

    /// A full run extended by a sparse one.
    TestColumn full_first(UUIDHelpers::generateV4(), "part_1", "col");
    const auto full_first_generation = cache.getInvalidationGeneration(full_first.table_uuid);
    EXPECT_GT(cache.setMany({makeEntry(full_first, 0, 0, 4)}, full_first_generation), 0u);
    EXPECT_GT(cache.setMany({makeSparseEntry(full_first, 0, 4, 8)}, full_first_generation), 0u);
    expect_whole_stripe(getOne(cache, full_first, 0));

    /// And a sparse run extended by a full one.
    TestColumn sparse_first(UUIDHelpers::generateV4(), "part_1", "col");
    const auto sparse_first_generation = cache.getInvalidationGeneration(sparse_first.table_uuid);
    EXPECT_GT(cache.setMany({makeSparseEntry(sparse_first, 0, 0, 4)}, sparse_first_generation), 0u);
    EXPECT_GT(cache.setMany({makeEntry(sparse_first, 0, 4, 8)}, sparse_first_generation), 0u);
    expect_whole_stripe(getOne(cache, sparse_first, 0));

    /// The run on the left of the resident one goes through the same copy.
    TestColumn on_the_left(UUIDHelpers::generateV4(), "part_1", "col");
    const auto on_the_left_generation = cache.getInvalidationGeneration(on_the_left.table_uuid);
    EXPECT_GT(cache.setMany({makeEntry(on_the_left, 0, 4, 8)}, on_the_left_generation), 0u);
    EXPECT_GT(cache.setMany({makeSparseEntry(on_the_left, 0, 0, 4)}, on_the_left_generation), 0u);
    expect_whole_stripe(getOne(cache, on_the_left, 0));
}

TEST(ColumnsCache, IndexIsErasedForAPartWhoseEntriesAreAllGone)
{
    /// The index of a part must not retain memory for entries the cache no longer holds: a bitmap
    /// sized by the highest stripe ever cached would keep a large allocation resident after a
    /// single cached tail granule of a large part was evicted.
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    /// One entry of a very high stripe, as a read of the tail of a large part writes.
    EXPECT_GT(cache.setMany({makeEntry(c, 1000000, 8000000, 8000008)}, generation), 0u);
    EXPECT_TRUE(cache.containsPart(c.table_uuid, "part_1"));

    cache.removePart(c.table_uuid, "part_1");
    EXPECT_FALSE(cache.containsPart(c.table_uuid, "part_1"));

    /// The same through ordinary eviction rather than an explicit removal: a cache that holds one
    /// entry at a time, filled with entries of ever higher stripes.
    auto small = makeCache(ColumnsCache::numberOfShards(8192) * 8192);
    TestColumn tail(UUIDHelpers::generateV4(), "part_2", "col");
    const auto tail_generation = small.getInvalidationGeneration(tail.table_uuid);
    for (size_t i = 0; i < 64; ++i)
    {
        const size_t stripe = 1000000 + i * 1000;
        small.setMany({makeEntry(tail, stripe, stripe * 8, stripe * 8 + 8)}, tail_generation);
    }
    /// Whatever is resident is in the index, and the index of the part goes away with it.
    EXPECT_EQ(small.containsPart(tail.table_uuid, "part_2"), small.count() > 0);
    small.removePart(tail.table_uuid, "part_2");
    EXPECT_EQ(small.count(), 0u);
    EXPECT_FALSE(small.containsPart(tail.table_uuid, "part_2"));
}

TEST(ColumnsCache, EntriesAreNotVisibleAcrossSchemaIdentities)
{
    auto cache = makeCache();
    const UUID table_uuid = UUIDHelpers::generateV4();
    TestColumn before(table_uuid, "part_1", "col", /*schema_identity=*/ 7);
    TestColumn after(table_uuid, "part_1", "col", /*schema_identity=*/ 8);
    const auto generation = cache.getInvalidationGeneration(table_uuid);

    EXPECT_GT(cache.setMany({makeEntry(before, 0, 0, 8)}, generation), 0u);

    /// The same reader repeating the read finds its entry.
    EXPECT_EQ(countPresent(cache, before, 0, 1), 1u);
    /// A reader with another schema identity does not.
    EXPECT_EQ(countPresent(cache, after, 0, 1), 0u);
}

TEST(ColumnsCache, DisabledCacheStillInvalidatesInFlightReaders)
{
    auto cache = makeCache();
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");

    /// A reader that started while the cache was enabled holds a token.
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    /// The cache is disabled by a config reload, the table is altered, and the cache is
    /// enabled again before the reader gets to its deferred write. The invalidation happened
    /// while the cache was disabled, but it still has to reject that write.
    cache.setConfiguredMaxSizeInBytes(0);
    cache.removeTable(c.table_uuid);
    cache.setConfiguredMaxSizeInBytes(1 << 24);

    EXPECT_EQ(cache.setMany({makeEntry(c, 0, 0, 8)}, generation), 0u);

    /// A reader that starts after all of it can write again.
    const auto fresh_generation = cache.getInvalidationGeneration(c.table_uuid);
    EXPECT_GT(cache.setMany({makeEntry(c, 0, 0, 8)}, fresh_generation), 0u);
}

TEST(ColumnsCache, DisabledCacheDoesNotAccumulatePerTableInvalidationMetadata)
{
    auto cache = makeCache(0);

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
    auto cache = makeCache();
    cache.setAutoResizeSettings(/*free_memory_ratio=*/ 0.0, /*history_window_ms=*/ 0);
    TestColumn c(UUIDHelpers::generateV4(), "part_1", "col");
    const auto generation = cache.getInvalidationGeneration(c.table_uuid);

    for (size_t stripe = 0; stripe < 100; ++stripe)
        EXPECT_GT(cache.setMany({makeEntry(c, stripe, stripe * 8, stripe * 8 + 8)}, generation), 0u);
    const size_t full_size = cache.sizeInBytes();
    ASSERT_EQ(cache.count(), 100u);

    /// The rest of the server uses all but a quarter of the limit: the cache shrinks to that
    /// quarter, whatever its configured size, and reports that the usage then fits.
    const size_t limit = 4 * full_size;
    const size_t others = 3 * full_size;
    EXPECT_TRUE(cache.autoResize(static_cast<Int64>(others + full_size), limit));
    EXPECT_EQ(cache.maxSizeInBytes(), full_size);

    /// The usage of the rest grows past what the limit leaves: entries are evicted.
    EXPECT_TRUE(cache.autoResize(static_cast<Int64>(limit - full_size / 2 + cache.sizeInBytes()), limit));
    EXPECT_LE(cache.sizeInBytes(), full_size / 2 + ColumnsCache::MAX_SHARDS * 16);
    EXPECT_LT(cache.count(), 100u);
    EXPECT_GT(cache.count(), 0u);

    /// Nothing else uses memory any more: the cache may grow back, but not beyond its
    /// configured size.
    EXPECT_TRUE(cache.autoResize(static_cast<Int64>(cache.sizeInBytes()), 100 << 20));
    EXPECT_EQ(cache.maxSizeInBytes(), 1u << 24);

    /// The usage of the rest exceeds the limit by itself: the cache gives up everything and
    /// reports that it is not enough.
    EXPECT_FALSE(cache.autoResize(static_cast<Int64>(2 * limit), limit));
    EXPECT_EQ(cache.count(), 0u);
}
