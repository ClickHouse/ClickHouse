#include <gtest/gtest.h>

#include <config.h>

#if USE_PARQUET

#include <Common/Exception.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

#include <limits>

using namespace DB;
using namespace DB::Parquet;

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{

parq::FileMetaData makeFileMetaData(Int64 file_num_rows, const std::vector<Int64> & row_group_num_rows)
{
    parq::FileMetaData meta;
    meta.__set_num_rows(file_num_rows);
    meta.row_groups.reserve(row_group_num_rows.size());
    for (Int64 num_rows : row_group_num_rows)
    {
        parq::RowGroup row_group;
        row_group.__set_num_rows(num_rows);
        meta.row_groups.push_back(std::move(row_group));
    }
    return meta;
}

}

TEST(ParquetRowGroupGlobalOffsets, BuildsPrefixOffsets)
{
    const auto offsets = buildRowGroupGlobalOffsets(makeFileMetaData(30, {10, 0, 20}));
    ASSERT_EQ(offsets.size(), 4u);
    EXPECT_EQ(offsets[0], 0u);
    EXPECT_EQ(offsets[1], 10u);
    EXPECT_EQ(offsets[2], 10u);
    EXPECT_EQ(offsets[3], 30u);
}

TEST(ParquetRowGroupGlobalOffsets, RejectsNegativeFileRowCount)
{
    EXPECT_THROW(buildRowGroupGlobalOffsets(makeFileMetaData(-1, {})), Exception);
}

TEST(ParquetRowGroupGlobalOffsets, RejectsNegativeRowGroupRowCount)
{
    EXPECT_THROW(buildRowGroupGlobalOffsets(makeFileMetaData(10, {10, -1})), Exception);
}

TEST(ParquetRowGroupGlobalOffsets, RejectsMismatchWithFileNumRows)
{
    try
    {
        buildRowGroupGlobalOffsets(makeFileMetaData(11, {10, 0}));
        FAIL() << "Expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(ParquetRowGroupGlobalOffsets, RejectsOverflowingCumulativeCounts)
{
    constexpr Int64 max_rows = std::numeric_limits<Int64>::max();
    /// Two Int64::max values fit in UInt64; the third overflows checked addition.
    try
    {
        buildRowGroupGlobalOffsets(makeFileMetaData(max_rows, {max_rows, max_rows, max_rows}));
        FAIL() << "Expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

#endif
