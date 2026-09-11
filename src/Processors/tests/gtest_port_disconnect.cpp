#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

}

TEST(Port, Disconnect)
{
    auto header = makeHeader();

    OutputPort out(header);
    InputPort in(header);

    connect(out, in);
    ASSERT_TRUE(out.isConnected());
    ASSERT_TRUE(in.isConnected());

    disconnect(out, in);
    EXPECT_FALSE(out.isConnected());
    EXPECT_FALSE(in.isConnected());
    EXPECT_FALSE(out.getUpdateChannel().isConnected());
    EXPECT_FALSE(in.getUpdateChannel().isConnected());
}
