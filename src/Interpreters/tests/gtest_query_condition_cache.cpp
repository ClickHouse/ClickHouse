#include <gtest/gtest.h>

#include <Interpreters/Cache/QueryConditionCache.h>
#include <Core/UUID.h>

using namespace DB;

/// File-backed storages (`File`, object storage) store the digest of the file-level metadata
/// (e.g. the Parquet footer) their matching marks were computed from, so the read path can refuse
/// to apply the marks to a file whose metadata diverged (see
/// `QueryConditionCache::Entry::file_metadata_digest`). The digest must round-trip with the entry,
/// must default to 0 ("unknown") for writers that pass none (MergeTree), and must stay immutable
/// once the entry exists: concurrent writers of the same key hold the same version token, so the
/// first writer's digest describes the same generation.
TEST(QueryConditionCache, FileMetadataDigestRoundTrip)
{
    QueryConditionCache cache("SLRU", /*max_size_in_bytes=*/1024 * 1024, /*size_ratio=*/0.5);

    const UUID table_id = UUIDHelpers::generateV4();
    const String part_name = QueryConditionCache::makeFilePartName("/data/file.parquet", "token");
    const UInt64 condition_hash = 42;

    /// Marks 1 and 3 do not match; 5 marks in total.
    MarkRanges unmatched_ranges;
    unmatched_ranges.push_back({1, 2});
    unmatched_ranges.push_back({3, 4});
    cache.write(table_id, part_name, condition_hash, "cond", unmatched_ranges, /*marks_count=*/5, /*has_final_mark=*/false, /*file_metadata_digest=*/0xfeed);

    UInt64 digest = 0;
    auto marks = cache.read(table_id, part_name, condition_hash, /*increment_profile_events=*/true, &digest);
    ASSERT_TRUE(marks.has_value());
    EXPECT_EQ(*marks, (QueryConditionCache::MatchingMarks{true, false, true, false, true}));
    EXPECT_EQ(digest, 0xfeedu);

    /// Reading without asking for the digest works as before.
    EXPECT_TRUE(cache.read(table_id, part_name, condition_hash).has_value());

    /// A second write of the same key must not change the digest (first writer wins).
    cache.write(table_id, part_name, condition_hash, "cond", unmatched_ranges, /*marks_count=*/5, /*has_final_mark=*/false, /*file_metadata_digest=*/0xbeef);
    digest = 0;
    ASSERT_TRUE(cache.read(table_id, part_name, condition_hash, /*increment_profile_events=*/true, &digest).has_value());
    EXPECT_EQ(digest, 0xfeedu);

    /// A writer that reports no digest (e.g. MergeTree) leaves it 0.
    const String other_part = QueryConditionCache::makeFilePartName("/data/other.parquet", "token");
    cache.write(table_id, other_part, condition_hash, "cond", unmatched_ranges, /*marks_count=*/5, /*has_final_mark=*/false);
    digest = 0xdead;
    ASSERT_TRUE(cache.read(table_id, other_part, condition_hash, /*increment_profile_events=*/true, &digest).has_value());
    EXPECT_EQ(digest, 0u);
}
