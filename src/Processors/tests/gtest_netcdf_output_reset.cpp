#include <gtest/gtest.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NetCDF.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/NetCDFOutputFormat.h>

#include <bit>
#include <cstring>

using namespace DB;

namespace
{

Block makeBlock(Int32 value, Int64 time)
{
    auto values = ColumnInt32::create();
    values->insertValue(value);

    /// The scale 1 has no name in the CF conventions, so the values are written in milliseconds,
    /// multiplied by 100 in `finalizeImpl`.
    auto times = ColumnDecimal<DateTime64>::create(0, 1);
    times->insertValue(DateTime64(time));

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(values), std::make_shared<DataTypeInt32>(), "v"));
    block.insert(ColumnWithTypeAndName(std::move(times), std::make_shared<DataTypeDateTime64>(1), "t"));
    return block;
}

template <typename T>
T readValue(const String & file, UInt64 offset)
{
    T value;
    memcpy(&value, file.data() + offset, sizeof(T));
    if constexpr (std::endian::native == std::endian::little)
        value = std::byteswap(value);
    return value;
}

}

/// A message queue writes a file per message with the same formatter, and resets it in between:
/// the second file must not contain the rows of the first one, nor rescale its values again.
TEST(NetCDFOutputFormat, ResetBetweenFiles)
{
    Block header = makeBlock(0, 0).cloneEmpty();
    WriteBufferFromOwnString buffer;
    NetCDFOutputFormat format(buffer, std::make_shared<const Block>(header));

    for (Int32 message = 1; message <= 2; ++message)
    {
        format.write(makeBlock(message, message * 10));
        format.finalize();
        buffer.finalize();
        String file = buffer.str();

        ReadBufferFromString in(file);
        NetCDFHeader netcdf_header = readNetCDFHeader(in);

        ASSERT_EQ(netcdf_header.variables.size(), 2);
        ASSERT_EQ(netcdf_header.dimensions[netcdf_header.variables[0].dimension_ids[0]].length, 1);
        ASSERT_EQ(file.size(), netcdf_header.variables[1].begin + sizeof(Int64));

        EXPECT_EQ(readValue<Int32>(file, netcdf_header.variables[0].begin), message);
        EXPECT_EQ(readValue<Int64>(file, netcdf_header.variables[1].begin), message * 1000);

        buffer.restart();
        format.resetFormatter();
    }
}
