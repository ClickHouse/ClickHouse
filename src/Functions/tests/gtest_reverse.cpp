#include <gtest/gtest.h>

#include <Functions/reverse.h>

using namespace DB;

TEST(ReverseImpl, VectorFixedUsesInputRowsCountForSingleByteValues)
{
    ColumnString::Chars data(4);
    data[0] = 1;
    data[1] = 2;
    data[2] = 3;
    data[3] = 4;

    ColumnString::Chars res_data;

    ReverseImpl::vectorFixed(data, 1, res_data, 3);

    /// The result holds exactly `input_rows_count` values, even though the source column has more.
    ASSERT_EQ(res_data.size(), 3u);
    EXPECT_EQ(res_data[0], 1);
    EXPECT_EQ(res_data[1], 2);
    EXPECT_EQ(res_data[2], 3);
}

TEST(ReverseImpl, VectorFixedSizesResultFromInputRowsCount)
{
    /// Three rows of FixedString(3), but only two rows are requested.
    ColumnString::Chars data(9);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<UInt8>(i + 1);

    ColumnString::Chars res_data;

    ReverseImpl::vectorFixed(data, 3, res_data, 2);

    ASSERT_EQ(res_data.size(), 6u);
    EXPECT_EQ(res_data[0], 3);
    EXPECT_EQ(res_data[1], 2);
    EXPECT_EQ(res_data[2], 1);
    EXPECT_EQ(res_data[3], 6);
    EXPECT_EQ(res_data[4], 5);
    EXPECT_EQ(res_data[5], 4);

    ReverseImpl::vectorFixed(data, 3, res_data, 0);
    EXPECT_EQ(res_data.size(), 0u);
}

TEST(ReverseImpl, VectorSizesResultFromInputRowsCount)
{
    /// Three String rows: "ab", "cde", "f" — only two rows are requested.
    const std::string rows = "abcdef";
    ColumnString::Chars data;
    data.insert(rows.begin(), rows.end());
    ColumnString::Offsets offsets;
    offsets.push_back(2);
    offsets.push_back(5);
    offsets.push_back(6);

    ColumnString::Chars res_data;
    ColumnString::Offsets res_offsets;

    ReverseImpl::vector(data, offsets, res_data, res_offsets, 2);

    ASSERT_EQ(res_offsets.size(), 2u);
    EXPECT_EQ(res_offsets[0], 2u);
    EXPECT_EQ(res_offsets[1], 5u);
    ASSERT_EQ(res_data.size(), 5u);
    EXPECT_EQ(std::string(res_data.begin(), res_data.end()), "baedc");

    ReverseImpl::vector(data, offsets, res_data, res_offsets, 0);
    EXPECT_EQ(res_offsets.size(), 0u);
    EXPECT_EQ(res_data.size(), 0u);
}
