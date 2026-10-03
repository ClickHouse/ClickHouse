#include <gtest/gtest.h>
#include "config.h"

#if USE_PARQUET

#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>

using namespace DB;

/// The object-storage query-condition-cache read path builds a bucket from the empty prototype
/// returned by `FormatFactory::getFileBucketInfo` and must thread the file's total row-group count
/// (the number of cached marks) into it. Otherwise `file_num_row_groups` stays 0 ("unknown") and the
/// read-path `checkFileMatchesBucketAssignment` guard is disabled, so an object overwritten with a
/// different number of row groups is read against a stale assignment instead of failing close.
TEST(ParquetFileBucketInfoFilter, CachePrototypeCarriesTotalRowGroupCount)
{
    /// Mirrors `FormatFactory::getFileBucketInfo("Parquet")`: an empty prototype with an unknown count.
    ParquetFileBucketInfo prototype;
    ASSERT_TRUE(prototype.row_group_ids.empty());
    ASSERT_EQ(prototype.file_num_row_groups, 0u);

    const std::vector<size_t> matching_row_groups = {0, 1, 2};
    auto filtered = prototype.filterByMatchingRowGroups(matching_row_groups, /*file_num_row_groups=*/8, /*file_metadata_digest=*/0);
    ASSERT_TRUE(filtered != nullptr);

    const auto * parquet = dynamic_cast<const ParquetFileBucketInfo *>(filtered.get());
    ASSERT_TRUE(parquet != nullptr);
    EXPECT_EQ(parquet->row_group_ids, matching_row_groups);
    /// Without threading the count through, this would be 0 and the fail-close check would be disabled.
    EXPECT_EQ(parquet->file_num_row_groups, 8u);
}

/// A caller that does not know the total row-group count passes 0, which must not clobber a count the
/// bucket already carries - e.g. a splitter-produced bucket on the cluster read path already holds the
/// footer count and its filtered result must keep it.
TEST(ParquetFileBucketInfoFilter, ZeroKeepsExistingRowGroupCount)
{
    ParquetFileBucketInfo bucket({0, 1, 2}, /*file_num_row_groups=*/8);

    auto filtered = bucket.filterByMatchingRowGroups({0, 2}, /*file_num_row_groups=*/0, /*file_metadata_digest=*/0);
    ASSERT_TRUE(filtered != nullptr);

    const auto * parquet = dynamic_cast<const ParquetFileBucketInfo *>(filtered.get());
    ASSERT_TRUE(parquet != nullptr);
    EXPECT_EQ(parquet->row_group_ids, (std::vector<size_t>{0, 2}));
    EXPECT_EQ(parquet->file_num_row_groups, 8u);
}

/// An explicit non-zero total takes precedence over whatever the prototype carried.
TEST(ParquetFileBucketInfoFilter, NonZeroTotalOverridesExisting)
{
    ParquetFileBucketInfo bucket({0, 1, 2}, /*file_num_row_groups=*/0);

    auto filtered = bucket.filterByMatchingRowGroups({1, 2}, /*file_num_row_groups=*/5, /*file_metadata_digest=*/0);
    ASSERT_TRUE(filtered != nullptr);

    const auto * parquet = dynamic_cast<const ParquetFileBucketInfo *>(filtered.get());
    ASSERT_TRUE(parquet != nullptr);
    EXPECT_EQ(parquet->row_group_ids, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(parquet->file_num_row_groups, 5u);
}

/// The query-condition-cache read paths pass the digest of the footer the cached marks were
/// computed from (stored with the cache entry), and the resulting pruning restriction must carry
/// it: it is what lets the read path refuse to apply the marks to a file whose footer diverged
/// (see `ParquetFileBucketInfo::footer_digest`). A caller passing 0 ("unknown") must not clobber a
/// digest the bucket already carries - e.g. a splitter-produced bucket holds the digest of the
/// footer the split was computed from.
TEST(ParquetFileBucketInfoFilter, CachePrototypeCarriesFileMetadataDigest)
{
    ParquetFileBucketInfo prototype;
    ASSERT_EQ(prototype.footer_digest, 0u);

    auto filtered = prototype.filterByMatchingRowGroups({0, 2}, /*file_num_row_groups=*/8, /*file_metadata_digest=*/0xfeed);
    ASSERT_TRUE(filtered != nullptr);

    const auto * parquet = dynamic_cast<const ParquetFileBucketInfo *>(filtered.get());
    ASSERT_TRUE(parquet != nullptr);
    EXPECT_EQ(parquet->footer_digest, 0xfeedu);
    EXPECT_TRUE(parquet->omitted_row_groups_are_pruned);

    /// 0 keeps the digest a non-empty bucket already carries.
    ParquetFileBucketInfo split_bucket({0, 1, 2}, /*file_num_row_groups=*/8);
    split_bucket.footer_digest = 0xbeef;
    auto filtered_split = split_bucket.filterByMatchingRowGroups({1}, /*file_num_row_groups=*/0, /*file_metadata_digest=*/0);
    ASSERT_TRUE(filtered_split != nullptr);
    const auto * parquet_split = dynamic_cast<const ParquetFileBucketInfo *>(filtered_split.get());
    ASSERT_TRUE(parquet_split != nullptr);
    EXPECT_EQ(parquet_split->footer_digest, 0xbeefu);
}

#endif
