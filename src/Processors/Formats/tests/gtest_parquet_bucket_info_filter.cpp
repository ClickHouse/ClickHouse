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
    auto filtered = prototype.filterByMatchingRowGroups(matching_row_groups, /*file_num_row_groups=*/8);
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

    auto filtered = bucket.filterByMatchingRowGroups({0, 2}, /*file_num_row_groups=*/0);
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

    auto filtered = bucket.filterByMatchingRowGroups({1, 2}, /*file_num_row_groups=*/5);
    ASSERT_TRUE(filtered != nullptr);

    const auto * parquet = dynamic_cast<const ParquetFileBucketInfo *>(filtered.get());
    ASSERT_TRUE(parquet != nullptr);
    EXPECT_EQ(parquet->row_group_ids, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(parquet->file_num_row_groups, 5u);
}

#endif
