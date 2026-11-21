#include <gtest/gtest.h>

#include <Interpreters/Cache/QueryConditionCache.h>
#include <Core/UUID.h>
#include <Common/FailPoint.h>

#include <algorithm>
#include <limits>
#include <thread>

using namespace DB;

namespace
{

/// A `MatchingMarks` entry of this many marks takes about 1 KiB, the match counts of the same marks about 16 KiB.
constexpr size_t MARKS = 8192;
constexpr size_t CACHE_SIZE = 64 * 1024;

String partName(size_t part)
{
    return "all_" + std::to_string(part) + "_" + std::to_string(part) + "_0";
}

void writeMatchingMarks(QueryConditionCache & cache, const UUID & table_id, size_t part)
{
    cache.write(table_id, partName(part), /*condition_hash=*/ 1, "", MarkRanges{MarkRange(1, MARKS)}, MARKS, /*has_final_mark=*/ false);
}

void writeMatchCounts(QueryConditionCache & cache, const UUID & table_id, UInt64 condition_hash)
{
    QueryConditionCache::MarkMatchCounts counts;
    for (size_t mark = 0; mark < MARKS; ++mark)
        counts.emplace_back(mark, mark == 0 ? 7 : 0);
    cache.writeMatchCounts(table_id, partName(0), condition_hash, "", counts, MARKS, /*has_final_mark=*/ false);
}

std::vector<UInt128> matchingMarksKeys(const QueryConditionCache & cache)
{
    std::vector<UInt128> keys;
    for (const auto & key_mapped : cache.dump())
        keys.push_back(key_mapped.key);
    std::sort(keys.begin(), keys.end());
    return keys;
}

}

/// The match counts take 16 times the memory of the `MatchingMarks` of the same marks and only save reading the marks that
/// match. So they only use the space the `MatchingMarks` leave free and must never evict `MatchingMarks`, in particular not
/// when the cache is nearly full of them.
TEST(QueryConditionCacheMatchCounts, NeverEvictMatchingMarks)
{
    for (const auto * policy : {"LRU", "SLRU"})
    {
        SCOPED_TRACE(policy);
        QueryConditionCache cache(policy, 2 * CACHE_SIZE, 0.5);
        const UUID table_id = UUIDHelpers::generateV4();

        /// About 120 KiB of `MatchingMarks`: the match counts of a condition (about 16 KiB) do not fit beside them.
        for (size_t part = 1; part <= 112; ++part)
            writeMatchingMarks(cache, table_id, part);
        const auto keys = matchingMarksKeys(cache);
        ASSERT_FALSE(keys.empty());

        for (UInt64 condition_hash = 101; condition_hash <= 103; ++condition_hash)
        {
            writeMatchCounts(cache, table_id, condition_hash);
            EXPECT_EQ(matchingMarksKeys(cache), keys);
            EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), condition_hash).has_value());
        }
    }
}

/// With room beside the `MatchingMarks`, the match counts are cached, and the least recently used give way to new ones.
TEST(QueryConditionCacheMatchCounts, UseTheFreeSpace)
{
    for (const auto * policy : {"LRU", "SLRU"})
    {
        SCOPED_TRACE(policy);
        QueryConditionCache cache(policy, CACHE_SIZE, 0.5);
        const UUID table_id = UUIDHelpers::generateV4();

        /// About 40 KiB of `MatchingMarks`: the free space has room for the match counts of one condition, not two.
        for (size_t part = 1; part <= 36; ++part)
            writeMatchingMarks(cache, table_id, part);
        const auto keys = matchingMarksKeys(cache);

        for (UInt64 condition_hash = 101; condition_hash <= 103; ++condition_hash)
        {
            writeMatchCounts(cache, table_id, condition_hash);
            EXPECT_EQ(matchingMarksKeys(cache), keys);
        }

        auto counts = cache.readMatchCounts(table_id, partName(0), 103);
        ASSERT_TRUE(counts.has_value());
        EXPECT_EQ((*counts)[0], 7);
        EXPECT_EQ((*counts)[1], 0);
        EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 101).has_value());
    }
}

/// A new `MatchingMarks` entry takes its space back from the match counts, and counts that do not fit are not cached.
TEST(QueryConditionCacheMatchCounts, MatchingMarksTakeTheirSpaceBack)
{
    for (const auto * policy : {"LRU", "SLRU"})
    {
        SCOPED_TRACE(policy);
        QueryConditionCache cache(policy, CACHE_SIZE, 0.5);
        const UUID table_id = UUIDHelpers::generateV4();

        for (size_t part = 1; part <= 36; ++part)
            writeMatchingMarks(cache, table_id, part);
        writeMatchCounts(cache, table_id, 101);
        ASSERT_TRUE(cache.readMatchCounts(table_id, partName(0), 101).has_value());

        /// About 54 KiB of `MatchingMarks` in total (60 KiB in debug builds): they all fit, the match counts do not fit
        /// any more.
        for (size_t part = 37; part <= 50; ++part)
            writeMatchingMarks(cache, table_id, part);
        EXPECT_EQ(cache.dump().size(), 50);
        EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 101).has_value());

        writeMatchCounts(cache, table_id, 102);
        EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 102).has_value());
        EXPECT_EQ(cache.dump().size(), 50);

        /// Without `MatchingMarks`, the match counts may use the whole size.
        cache.clear();
        writeMatchCounts(cache, table_id, 103);
        EXPECT_TRUE(cache.readMatchCounts(table_id, partName(0), 103).has_value());
    }
}

/// Match counts are cached in two steps: their size is checked against the free space, then they are inserted. A
/// `MatchingMarks` entry inserted in between finds no match counts yet and does not take its space back from them, so the
/// insertion of the match counts must account for it. Here they do not fit together any more after the interleaving: all
/// `MatchingMarks` must stay, and the match counts must not stay above the size beside them.
TEST(QueryConditionCacheMatchCounts, MatchingMarksInsertedWhileCachingMatchCounts)
{
    static constexpr auto failpoint = "query_condition_cache_pause_before_caching_match_counts";
    for (const auto * policy : {"LRU", "SLRU"})
    {
        SCOPED_TRACE(policy);
        QueryConditionCache cache(policy, CACHE_SIZE, 0.5);
        const UUID table_id = UUIDHelpers::generateV4();

        /// About 40 KiB of `MatchingMarks`: the match counts of a condition (about 16 KiB) fit beside them.
        for (size_t part = 1; part <= 36; ++part)
            writeMatchingMarks(cache, table_id, part);

        FailPointInjection::enableFailPoint(failpoint);
        std::thread counts_writer([&] { writeMatchCounts(cache, table_id, 101); });
        FailPointInjection::waitForPause(failpoint);

        /// About 54 KiB of `MatchingMarks` in total, inserted while the match counts wait: these do not fit beside them any more.
        for (size_t part = 37; part <= 50; ++part)
            writeMatchingMarks(cache, table_id, part);

        FailPointInjection::disableFailPoint(failpoint);
        counts_writer.join();

        EXPECT_EQ(cache.dump().size(), 50);
        EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 101).has_value());

        /// Where they fit, the match counts are cached as usual.
        cache.clear();
        writeMatchCounts(cache, table_id, 102);
        EXPECT_TRUE(cache.readMatchCounts(table_id, partName(0), 102).has_value());
    }
}

/// Resizing the cache keeps the match counts within the space the `MatchingMarks` leave free, and clearing it removes both.
TEST(QueryConditionCacheMatchCounts, ResizeAndClear)
{
    for (const auto * policy : {"LRU", "SLRU"})
    {
        SCOPED_TRACE(policy);
        QueryConditionCache cache(policy, CACHE_SIZE, 0.5);
        const UUID table_id = UUIDHelpers::generateV4();

        for (size_t part = 1; part <= 36; ++part)
            writeMatchingMarks(cache, table_id, part);
        writeMatchCounts(cache, table_id, 101);
        ASSERT_TRUE(cache.readMatchCounts(table_id, partName(0), 101).has_value());

        /// The `MatchingMarks` still fit into the smaller size, the match counts do not fit beside them.
        cache.setMaxSizeInBytes(48 * 1024);
        EXPECT_EQ(cache.dump().size(), 36);
        EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 101).has_value());

        cache.setMaxSizeInBytes(CACHE_SIZE);
        writeMatchCounts(cache, table_id, 102);
        EXPECT_TRUE(cache.readMatchCounts(table_id, partName(0), 102).has_value());
        EXPECT_EQ(cache.dump().size(), 36);

        cache.clear();
        EXPECT_TRUE(cache.dump().empty());
        EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 102).has_value());
    }
}

/// Only a count below `UNKNOWN_MATCH_COUNT` is cached, however large the count is.
TEST(QueryConditionCacheMatchCounts, CountsAtOrAboveTheLimitAreNotCached)
{
    QueryConditionCache cache("LRU", CACHE_SIZE, 0.5);
    const UUID table_id = UUIDHelpers::generateV4();

    QueryConditionCache::MarkMatchCounts counts{{0, 65534}, {1, 65535}, {2, (1ULL << 32) + 7}};
    cache.writeMatchCounts(table_id, partName(0), /*condition_hash=*/ 1, "", counts, /*marks_count=*/ 4, /*has_final_mark=*/ false);

    auto cached = cache.readMatchCounts(table_id, partName(0), 1);
    ASSERT_TRUE(cached.has_value());
    EXPECT_EQ((*cached)[0], 65534);
    EXPECT_EQ((*cached)[1], QueryConditionCache::UNKNOWN_MATCH_COUNT);
    EXPECT_EQ((*cached)[2], QueryConditionCache::UNKNOWN_MATCH_COUNT);
    EXPECT_EQ((*cached)[3], QueryConditionCache::UNKNOWN_MATCH_COUNT);
}

/// Match counts that cannot fit into the cache are not allocated: here the allocation itself would throw.
TEST(QueryConditionCacheMatchCounts, CountsThatCannotFitAreNotAllocated)
{
    QueryConditionCache cache("LRU", CACHE_SIZE, 0.5);
    const UUID table_id = UUIDHelpers::generateV4();
    const QueryConditionCache::MarkMatchCounts counts{{0, 1}};

    const size_t marks_count_above_vector_max_size = std::numeric_limits<size_t>::max() / 2 + 1;
    EXPECT_NO_THROW(cache.writeMatchCounts(
        table_id, partName(0), /*condition_hash=*/ 1, "", counts, marks_count_above_vector_max_size, /*has_final_mark=*/ false));
    EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 1).has_value());

    /// Twice the size of the cache.
    cache.writeMatchCounts(table_id, partName(0), /*condition_hash=*/ 2, "", counts, CACHE_SIZE, /*has_final_mark=*/ false);
    EXPECT_FALSE(cache.readMatchCounts(table_id, partName(0), 2).has_value());
}
