#include <gtest/gtest.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NetCDF.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/NetCDFOutputFormat.h>
#include <Common/transformEndianness.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

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

namespace
{

template <typename T>
T decodeFillValue(const String & fill_value)
{
    EXPECT_EQ(fill_value.size(), sizeof(T));
    T value;
    memcpy(&value, fill_value.data(), sizeof(T));
    transformEndianness<std::endian::native, std::endian::big>(value);
    return value;
}

}

/// A reader compares a floating-point sentinel by value, so the fill value must not equal any value
/// of the data by value: `-0.0` takes `+0.0`, and a NaN in the data takes nothing.
TEST(NetCDFOutputFormat, FloatFillValueByValue)
{
    {
        /// `+0.0` is preferred, and `-0.0` is in the data.
        std::vector<Float32> data{-0.0f};
        Float32 fill = decodeFillValue<Float32>(chooseNetCDFFillValue<Float32>(data.data(), data.size(), nullptr, 0.0f, false));
        EXPECT_NE(fill, 0.0f);
        EXPECT_FALSE(std::isnan(fill));
    }
    {
        /// The candidates below the smallest positive denormal are `+0.0`, which equals the `-0.0`
        /// of the data, and then the negative values, never a NaN.
        Float32 denormal = std::numeric_limits<Float32>::denorm_min();
        std::vector<Float32> data{denormal, -0.0f};
        Float32 fill = decodeFillValue<Float32>(chooseNetCDFFillValue<Float32>(data.data(), data.size(), nullptr, denormal, false));
        EXPECT_EQ(fill, -denormal);
    }
    {
        /// Below the negative infinity, the search wraps around to the positive infinity.
        Float64 inf = std::numeric_limits<Float64>::infinity();
        std::vector<Float64> data{-inf, std::nan("")};
        Float64 fill = decodeFillValue<Float64>(chooseNetCDFFillValue<Float64>(data.data(), data.size(), nullptr, -inf, false));
        EXPECT_EQ(fill, inf);
    }
    {
        /// A NaN in the data does not take the preferred value.
        std::vector<Float64> data{std::nan(""), -std::nan("")};
        EXPECT_EQ(chooseNetCDFFillValue<Float64>(data.data(), data.size(), nullptr, 1.0, true), "");
    }
}

/// The integers are compared by their bytes, and the search wraps around the domain.
TEST(NetCDFOutputFormat, IntegerFillValueWrapsAround)
{
    std::vector<UInt8> data{0, 255};
    EXPECT_EQ(decodeFillValue<UInt8>(chooseNetCDFFillValue<UInt8>(data.data(), data.size(), nullptr, 0, false)), 254);

    std::vector<Int64> wide{std::numeric_limits<Int64>::min()};
    EXPECT_EQ(
        decodeFillValue<Int64>(chooseNetCDFFillValue<Int64>(wide.data(), wide.size(), nullptr, std::numeric_limits<Int64>::min(), false)),
        std::numeric_limits<Int64>::max());
}
