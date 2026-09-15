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

    constexpr UInt8 sentinel = 0xFF;
    ColumnString::Chars res_data(data.size(), sentinel);

    ReverseImpl::vectorFixed(data, 1, res_data, 3);

    EXPECT_EQ(res_data[0], 1);
    EXPECT_EQ(res_data[1], 2);
    EXPECT_EQ(res_data[2], 3);
    EXPECT_EQ(res_data[3], sentinel);
}
