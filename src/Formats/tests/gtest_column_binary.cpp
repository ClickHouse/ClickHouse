/// Unit tests for the ColumnBinary wire format.

#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Processors/Formats/Impl/ColumnBinaryOutputFormat.h>
#include <Processors/Formats/Impl/ColumnBinaryInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>

#include <Formats/FormatSettings.h>
#include <Formats/ColumnBinaryWire.h>

#include <Common/typeid_cast.h>

#include <cstring>

using namespace DB;

namespace
{
std::string getStringAt(const ColumnString & col, size_t n)
{
    auto at = col.getDataAt(n);
    return std::string(at.data(), at.size());
}

ColumnPtr makeColUInt64(const std::vector<UInt64> & data)
{
    auto col = ColumnUInt64::create();
    auto & d = col->getData();
    d.reserve(data.size());
    for (auto v : data)
        d.push_back(v);
    return ColumnPtr(std::move(col));
}

ColumnPtr makeArrayColumn(const ColumnPtr & data_col, const std::vector<UInt64> & offsets)
{
    auto offsets_col = ColumnVector<UInt64>::create();
    auto & od = offsets_col->getData();
    od.reserve(offsets.size());
    for (auto v : offsets)
        od.push_back(v);
    auto offsets_ptr = ColumnPtr(std::move(offsets_col));
    return ColumnArray::create(data_col, offsets_ptr);
}

ColumnPtr makeConstArrayColumn(const ColumnPtr & data_col, const std::vector<UInt64> & offsets, size_t repeat)
{
    auto offsets_col = ColumnVector<UInt64>::create();
    auto & od = offsets_col->getData();
    od.reserve(offsets.size());
    for (auto v : offsets)
        od.push_back(v);
    auto offsets_ptr = ColumnPtr(std::move(offsets_col));
    auto arr = ColumnArray::create(data_col, offsets_ptr);
    auto arr_ptr = ColumnPtr(std::move(arr));
    return ColumnConst::create(arr_ptr, repeat);
}
}

TEST(ColumnBinary, UInt64RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeUInt64>()};
    auto col = ColumnUInt64::create();
    auto & d = col->getData();
    d.push_back(100ULL); d.push_back(200ULL); d.push_back(300ULL);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnUInt64 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], 100ULL);
        EXPECT_EQ(decoded.getData()[1], 200ULL);
        EXPECT_EQ(decoded.getData()[2], 300ULL);
    }
}

TEST(ColumnBinary, Int32RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt32>()};
    auto col = ColumnInt32::create();
    auto & d = col->getData();
    d.push_back(-1); d.push_back(0); d.push_back(42); d.push_back(-100);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 4u);
        const auto & decoded = typeid_cast<const ColumnInt32 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], -1);
        EXPECT_EQ(decoded.getData()[1], 0);
        EXPECT_EQ(decoded.getData()[2], 42);
        EXPECT_EQ(decoded.getData()[3], -100);
    }
}

TEST(ColumnBinary, Float64RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeFloat64>()};
    auto col = ColumnFloat64::create();
    auto & d = col->getData();
    d.push_back(1.5); d.push_back(-2.5); d.push_back(0.0); d.push_back(3.75);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 4u);
        const auto & decoded = typeid_cast<const ColumnFloat64 &>(*chunk.getColumns()[0]);
        EXPECT_DOUBLE_EQ(decoded.getData()[0], 1.5);
        EXPECT_DOUBLE_EQ(decoded.getData()[1], -2.5);
        EXPECT_DOUBLE_EQ(decoded.getData()[2], 0.0);
        EXPECT_DOUBLE_EQ(decoded.getData()[3], 3.75);
    }
}

TEST(ColumnBinary, StringRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto col = ColumnString::create();
    col->insertData("hello", 5);
    col->insertData("world", 5);
    col->insertData("", 0);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnString &>(*chunk.getColumns()[0]);
        EXPECT_EQ(getStringAt(decoded, 0), "hello");
        EXPECT_EQ(getStringAt(decoded, 1), "world");
        EXPECT_EQ(getStringAt(decoded, 2), "");
    }
}

TEST(ColumnBinary, ConstColumnRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt32>()};
    auto inner_col = ColumnInt32::create();
    inner_col->insert(42);
    auto inner_ptr = ColumnPtr(std::move(inner_col));
    auto col = ColumnConst::create(inner_ptr, 5);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 5u);
        const auto & decoded = typeid_cast<const ColumnConst &>(*chunk.getColumns()[0]);
        const auto & decoded_inner = typeid_cast<const ColumnInt32 &>(decoded.getDataColumn());
        EXPECT_EQ(decoded_inner.getData()[0], 42);
    }
}

TEST(ColumnBinary, NullableRoundTrip)
{
    auto nested_type = std::make_shared<DataTypeInt64>();
    DataTypes types = {std::make_shared<DataTypeNullable>(nested_type)};
    auto col = ColumnNullable::create(
        ColumnInt64::create(),
        ColumnUInt8::create());
    auto & nd = typeid_cast<ColumnInt64 &>(col->getNestedColumn()).getData();
    nd.push_back(100); nd.push_back(200); nd.push_back(300);
    auto & nm = typeid_cast<ColumnUInt8 &>(col->getNullMapColumn()).getData();
    nm.push_back(static_cast<UInt8>(0)); nm.push_back(static_cast<UInt8>(1)); nm.push_back(static_cast<UInt8>(0));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnNullable &>(*chunk.getColumns()[0]);
        const auto & nested = typeid_cast<const ColumnInt64 &>(decoded.getNestedColumn());
        EXPECT_EQ(nested.getData()[0], 100);
        EXPECT_EQ(nested.getData()[1], 200);
        EXPECT_EQ(nested.getData()[2], 300);
        const auto & null_map = typeid_cast<const ColumnUInt8 &>(decoded.getNullMapColumn());
        EXPECT_EQ(null_map.getData()[0], 0);
        EXPECT_EQ(null_map.getData()[1], 1);
        EXPECT_EQ(null_map.getData()[2], 0);
    }
}

TEST(ColumnBinary, ArrayRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    auto data_col = makeColUInt64({1, 2, 3, 4, 5, 6});
    auto col = makeArrayColumn(data_col, {1, 3, 6});
    Block header;
    header.insert(ColumnWithTypeAndName{col, types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnArray &>(*chunk.getColumns()[0]);
        const auto & data = typeid_cast<const ColumnUInt64 &>(decoded.getData());
        const auto & offsets = decoded.getOffsets();
        EXPECT_EQ(offsets[0], 1);
        EXPECT_EQ(offsets[1], 3);
        EXPECT_EQ(offsets[2], 6);
        EXPECT_EQ(data.getData()[0], 1);
        EXPECT_EQ(data.getData()[1], 2);
        EXPECT_EQ(data.getData()[2], 3);
        EXPECT_EQ(data.getData()[3], 4);
        EXPECT_EQ(data.getData()[4], 5);
        EXPECT_EQ(data.getData()[5], 6);
    }
}

TEST(ColumnBinary, TupleRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeString>()})};
    auto col_u32 = ColumnUInt32::create();
    col_u32->getData().push_back(10);
    col_u32->getData().push_back(20);
    col_u32->getData().push_back(30);
    auto col_str = ColumnString::create();
    col_str->insertData("a", 1);
    col_str->insertData("b", 1);
    col_str->insertData("c", 1);
    auto col_u32_ptr = ColumnPtr(std::move(col_u32));
    auto col_str_ptr = ColumnPtr(std::move(col_str));
    Columns tuple_cols;
    tuple_cols.push_back(std::move(col_u32_ptr));
    tuple_cols.push_back(std::move(col_str_ptr));
    auto col = ColumnTuple::create(tuple_cols);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnTuple &>(*chunk.getColumns()[0]);
        const auto & decoded_u32 = typeid_cast<const ColumnUInt32 &>(decoded.getColumn(0));
        EXPECT_EQ(decoded_u32.getData()[0], 10);
        EXPECT_EQ(decoded_u32.getData()[1], 20);
        EXPECT_EQ(decoded_u32.getData()[2], 30);
        const auto & decoded_str = typeid_cast<const ColumnString &>(decoded.getColumn(1));
        EXPECT_EQ(getStringAt(decoded_str, 0), "a");
        EXPECT_EQ(getStringAt(decoded_str, 1), "b");
        EXPECT_EQ(getStringAt(decoded_str, 2), "c");
    }
}

TEST(ColumnBinary, MultiColumnRoundTrip)
{
    DataTypes types = {
        std::make_shared<DataTypeInt32>(),
        std::make_shared<DataTypeFloat64>(),
        std::make_shared<DataTypeString>()
    };
    Block header;
    {
        auto col0 = ColumnInt32::create();
        col0->getData().push_back(1);
        col0->getData().push_back(2);
        col0->getData().push_back(3);
        header.insert(ColumnWithTypeAndName{std::move(col0), types[0], "int_col"});
    }
    {
        auto col1 = ColumnFloat64::create();
        col1->getData().push_back(1.1);
        col1->getData().push_back(2.2);
        col1->getData().push_back(3.3);
        header.insert(ColumnWithTypeAndName{std::move(col1), types[1], "float_col"});
    }
    {
        auto col2 = ColumnString::create();
        col2->insertData("x", 1);
        col2->insertData("y", 1);
        col2->insertData("z", 1);
        header.insert(ColumnWithTypeAndName{std::move(col2), types[2], "str_col"});
    }

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 3u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & col0 = typeid_cast<const ColumnInt32 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(col0.getData()[0], 1);
        EXPECT_EQ(col0.getData()[1], 2);
        EXPECT_EQ(col0.getData()[2], 3);
        const auto & col1 = typeid_cast<const ColumnFloat64 &>(*chunk.getColumns()[1]);
        EXPECT_DOUBLE_EQ(col1.getData()[0], 1.1);
        EXPECT_DOUBLE_EQ(col1.getData()[1], 2.2);
        EXPECT_DOUBLE_EQ(col1.getData()[2], 3.3);
        const auto & col2 = typeid_cast<const ColumnString &>(*chunk.getColumns()[2]);
        EXPECT_EQ(getStringAt(col2, 0), "x");
        EXPECT_EQ(getStringAt(col2, 1), "y");
        EXPECT_EQ(getStringAt(col2, 2), "z");
    }
}

TEST(ColumnBinary, ConstMultiColumnRoundTrip)
{
    DataTypes types = {
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeString>()
    };
    Block header;
    {
        auto inner = ColumnInt64::create();
        inner->insert(999);
        auto inner_ptr = ColumnPtr(std::move(inner));
    auto col = ColumnConst::create(inner_ptr, 3);
        header.insert(ColumnWithTypeAndName{std::move(col), types[0], "const_int"});
    }
    {
        auto col = ColumnString::create();
        col->insertData("a", 1);
        col->insertData("b", 1);
        col->insertData("c", 1);
        header.insert(ColumnWithTypeAndName{std::move(col), types[1], "str_col"});
    }

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 2u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & col0 = typeid_cast<const ColumnConst &>(*chunk.getColumns()[0]);
        const auto & inner = typeid_cast<const ColumnInt64 &>(col0.getDataColumn());
        EXPECT_EQ(inner.getData()[0], 999);
        const auto & col1 = typeid_cast<const ColumnString &>(*chunk.getColumns()[1]);
        EXPECT_EQ(getStringAt(col1, 0), "a");
        EXPECT_EQ(getStringAt(col1, 1), "b");
        EXPECT_EQ(getStringAt(col1, 2), "c");
    }
}

TEST(ColumnBinary, UInt8UInt16RoundTrip)
{
    DataTypes types = {
        std::make_shared<DataTypeUInt8>(),
        std::make_shared<DataTypeUInt16>()
    };
    Block header;
    {
        auto col = ColumnUInt8::create();
        col->getData().push_back(static_cast<UInt8>(1));
        col->getData().push_back(static_cast<UInt8>(2));
        col->getData().push_back(static_cast<UInt8>(3));
        header.insert(ColumnWithTypeAndName{std::move(col), types[0], "u8_col"});
    }
    {
        auto col = ColumnUInt16::create();
        col->getData().push_back(static_cast<UInt16>(100));
        col->getData().push_back(static_cast<UInt16>(200));
        col->getData().push_back(static_cast<UInt16>(300));
        header.insert(ColumnWithTypeAndName{std::move(col), types[1], "u16_col"});
    }

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 2u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & col0 = typeid_cast<const ColumnUInt8 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(col0.getData()[0], 1);
        EXPECT_EQ(col0.getData()[1], 2);
        EXPECT_EQ(col0.getData()[2], 3);
        const auto & col1 = typeid_cast<const ColumnUInt16 &>(*chunk.getColumns()[1]);
        EXPECT_EQ(col1.getData()[0], 100);
        EXPECT_EQ(col1.getData()[1], 200);
        EXPECT_EQ(col1.getData()[2], 300);
    }
}

TEST(ColumnBinary, Float32RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeFloat32>()};
    auto col = ColumnFloat32::create();
    col->getData().push_back(1.5f);
    col->getData().push_back(-2.5f);
    col->getData().push_back(0.0f);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnFloat32 &>(*chunk.getColumns()[0]);
        EXPECT_FLOAT_EQ(decoded.getData()[0], 1.5f);
        EXPECT_FLOAT_EQ(decoded.getData()[1], -2.5f);
        EXPECT_FLOAT_EQ(decoded.getData()[2], 0.0f);
    }
}

TEST(ColumnBinary, Int8Int16RoundTrip)
{
    DataTypes types = {
        std::make_shared<DataTypeInt8>(),
        std::make_shared<DataTypeInt16>()
    };
    Block header;
    {
        auto col = ColumnInt8::create();
        col->getData().push_back(static_cast<Int8>(-1));
        col->getData().push_back(static_cast<Int8>(0));
        col->getData().push_back(static_cast<Int8>(127));
        header.insert(ColumnWithTypeAndName{std::move(col), types[0], "i8_col"});
    }
    {
        auto col = ColumnInt16::create();
        col->getData().push_back(static_cast<Int16>(-32768));
        col->getData().push_back(static_cast<Int16>(0));
        col->getData().push_back(static_cast<Int16>(32767));
        header.insert(ColumnWithTypeAndName{std::move(col), types[1], "i16_col"});
    }

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 2u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & col0 = typeid_cast<const ColumnInt8 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(col0.getData()[0], -1);
        EXPECT_EQ(col0.getData()[1], 0);
        EXPECT_EQ(col0.getData()[2], 127);
        const auto & col1 = typeid_cast<const ColumnInt16 &>(*chunk.getColumns()[1]);
        EXPECT_EQ(col1.getData()[0], -32768);
        EXPECT_EQ(col1.getData()[1], 0);
        EXPECT_EQ(col1.getData()[2], 32767);
    }
}

TEST(ColumnBinary, UInt32RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeUInt32>()};
    auto col = ColumnUInt32::create();
    col->getData().push_back(0);
    col->getData().push_back(1);
    col->getData().push_back(4294967295u);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnUInt32 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], 0u);
        EXPECT_EQ(decoded.getData()[1], 1u);
        EXPECT_EQ(decoded.getData()[2], 4294967295u);
    }
}

TEST(ColumnBinary, Int64RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt64>()};
    auto col = ColumnInt64::create();
    col->getData().push_back(-9223372036854775807LL);
    col->getData().push_back(0LL);
    col->getData().push_back(9223372036854775807LL);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnInt64 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], -9223372036854775807LL);
        EXPECT_EQ(decoded.getData()[1], 0LL);
        EXPECT_EQ(decoded.getData()[2], 9223372036854775807LL);
    }
}

TEST(ColumnBinary, ConstStringRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto inner = ColumnString::create();
    inner->insertData("constant", 8);
    auto inner_ptr = ColumnPtr(std::move(inner));
    auto col = ColumnConst::create(inner_ptr, 4);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 4u);
        const auto & decoded = typeid_cast<const ColumnConst &>(*chunk.getColumns()[0]);
        const auto & inner_decoded = typeid_cast<const ColumnString &>(decoded.getDataColumn());
        EXPECT_EQ(getStringAt(inner_decoded, 0), "constant");
    }
}

TEST(ColumnBinary, ConstNullableRoundTrip)
{
    auto nested_type = std::make_shared<DataTypeInt32>();
    DataTypes types = {std::make_shared<DataTypeNullable>(nested_type)};
    auto nested_col = ColumnInt32::create();
    nested_col->insert(42);
    auto null_map = ColumnUInt8::create();
    null_map->insert(0);
    auto nullable = ColumnNullable::create(std::move(nested_col), std::move(null_map));
    auto nullable_ptr = ColumnPtr(std::move(nullable));
    auto col = ColumnConst::create(nullable_ptr, 3);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnConst &>(*chunk.getColumns()[0]);
        const auto & decoded_nullable = typeid_cast<const ColumnNullable &>(decoded.getDataColumn());
        const auto & inner = typeid_cast<const ColumnInt32 &>(decoded_nullable.getNestedColumn());
        EXPECT_EQ(inner.getData()[0], 42);
    }
}

TEST(ColumnBinary, ArrayOfUInt64RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    auto data_col = makeColUInt64({10, 20, 30, 40, 50});
    auto col = makeArrayColumn(data_col, {1, 3, 4, 5});
    Block header;
    header.insert(ColumnWithTypeAndName{col, types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 4u);
        const auto & decoded = typeid_cast<const ColumnArray &>(*chunk.getColumns()[0]);
        const auto & data = typeid_cast<const ColumnUInt64 &>(decoded.getData());
        const auto & offsets = decoded.getOffsets();
        EXPECT_EQ(offsets[0], 1);
        EXPECT_EQ(offsets[1], 3);
        EXPECT_EQ(offsets[2], 4);
        EXPECT_EQ(offsets[3], 5);
        EXPECT_EQ(data.getData()[0], 10);
        EXPECT_EQ(data.getData()[1], 20);
        EXPECT_EQ(data.getData()[2], 30);
        EXPECT_EQ(data.getData()[3], 40);
        EXPECT_EQ(data.getData()[4], 50);
    }
}

TEST(ColumnBinary, TupleOfThreeRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeUInt32>(),
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeFloat64>()
        })};
    auto col_u32 = ColumnUInt32::create();
    col_u32->getData().push_back(1);
    col_u32->getData().push_back(2);
    auto col_str = ColumnString::create();
    col_str->insertData("a", 1);
    col_str->insertData("b", 1);
    auto col_f64 = ColumnFloat64::create();
    col_f64->getData().push_back(1.1);
    col_f64->getData().push_back(2.2);
    auto col_u32_ptr = ColumnPtr(std::move(col_u32));
    auto col_str_ptr = ColumnPtr(std::move(col_str));
    auto col_f64_ptr = ColumnPtr(std::move(col_f64));
    Columns tuple_cols;
    tuple_cols.push_back(std::move(col_u32_ptr));
    tuple_cols.push_back(std::move(col_str_ptr));
    tuple_cols.push_back(std::move(col_f64_ptr));
    auto col = ColumnTuple::create(tuple_cols);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 2u);
        const auto & decoded = typeid_cast<const ColumnTuple &>(*chunk.getColumns()[0]);
        const auto & u32 = typeid_cast<const ColumnUInt32 &>(decoded.getColumn(0));
        EXPECT_EQ(u32.getData()[0], 1);
        EXPECT_EQ(u32.getData()[1], 2);
        const auto & str = typeid_cast<const ColumnString &>(decoded.getColumn(1));
        EXPECT_EQ(getStringAt(str, 0), "a");
        EXPECT_EQ(getStringAt(str, 1), "b");
        const auto & f64 = typeid_cast<const ColumnFloat64 &>(decoded.getColumn(2));
        EXPECT_DOUBLE_EQ(f64.getData()[0], 1.1);
        EXPECT_DOUBLE_EQ(f64.getData()[1], 2.2);
    }
}

TEST(ColumnBinary, ConstArrayRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    auto data_col = makeColUInt64({1, 2, 3});
    auto col = makeConstArrayColumn(data_col, {3}, 2);
    Block header;
    header.insert(ColumnWithTypeAndName{col, types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 2u);
        const auto & decoded = typeid_cast<const ColumnConst &>(*chunk.getColumns()[0]);
        const auto & inner_decoded = typeid_cast<const ColumnArray &>(decoded.getDataColumn());
        const auto & data = typeid_cast<const ColumnUInt64 &>(inner_decoded.getData());
        const auto & offsets = inner_decoded.getOffsets();
        EXPECT_EQ(offsets[0], 3);
        EXPECT_EQ(data.getData()[0], 1);
        EXPECT_EQ(data.getData()[1], 2);
        EXPECT_EQ(data.getData()[2], 3);
    }
}

TEST(ColumnBinary, EmptyStringsRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto col = ColumnString::create();
    col->insertData("", 0);
    col->insertData("", 0);
    col->insertData("", 0);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnString &>(*chunk.getColumns()[0]);
        EXPECT_EQ(getStringAt(decoded, 0), "");
        EXPECT_EQ(getStringAt(decoded, 1), "");
        EXPECT_EQ(getStringAt(decoded, 2), "");
    }
}

TEST(ColumnBinary, SingleRowRoundTrip)
{
    DataTypes types = {
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeString>()
    };
    Block header;
    {
        auto col = ColumnUInt64::create();
        col->getData().push_back(42ULL);
        header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});
    }
    {
        auto col = ColumnString::create();
        col->insertData("single", 6);
        header.insert(ColumnWithTypeAndName{std::move(col), types[1], "col1"});
    }

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 2u);
        ASSERT_EQ(chunk.getNumRows(), 1u);
        const auto & col0 = typeid_cast<const ColumnUInt64 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(col0.getData()[0], 42ULL);
        const auto & col1 = typeid_cast<const ColumnString &>(*chunk.getColumns()[1]);
        EXPECT_EQ(getStringAt(col1, 0), "single");
    }
}

TEST(ColumnBinary, NullableAllNullsRoundTrip)
{
    auto nested_type = std::make_shared<DataTypeInt32>();
    DataTypes types = {std::make_shared<DataTypeNullable>(nested_type)};
    auto col = ColumnNullable::create(
        ColumnInt32::create(),
        ColumnUInt8::create());
    typeid_cast<ColumnInt32 &>(col->getNestedColumn()).getData().push_back(0);
    typeid_cast<ColumnInt32 &>(col->getNestedColumn()).getData().push_back(0);
    typeid_cast<ColumnInt32 &>(col->getNestedColumn()).getData().push_back(0);
  typeid_cast<ColumnUInt8 &>(col->getNullMapColumn()).getData().push_back(static_cast<UInt8>(1));
   typeid_cast<ColumnUInt8 &>(col->getNullMapColumn()).getData().push_back(static_cast<UInt8>(1));
   typeid_cast<ColumnUInt8 &>(col->getNullMapColumn()).getData().push_back(static_cast<UInt8>(1));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnNullable &>(*chunk.getColumns()[0]);
        const auto & null_map = typeid_cast<const ColumnUInt8 &>(decoded.getNullMapColumn());
        EXPECT_EQ(null_map.getData()[0], 1);
        EXPECT_EQ(null_map.getData()[1], 1);
        EXPECT_EQ(null_map.getData()[2], 1);
    }
}

TEST(ColumnBinary, NullableNoNullsRoundTrip)
{
    auto nested_type = std::make_shared<DataTypeInt32>();
    DataTypes types = {std::make_shared<DataTypeNullable>(nested_type)};
    auto col = ColumnNullable::create(
        ColumnInt32::create(),
        ColumnUInt8::create());
    auto & nd = typeid_cast<ColumnInt32 &>(col->getNestedColumn()).getData();
    nd.push_back(100); nd.push_back(200); nd.push_back(300);
    auto & nm = typeid_cast<ColumnUInt8 &>(col->getNullMapColumn()).getData();
    nm.push_back(static_cast<UInt8>(0)); nm.push_back(static_cast<UInt8>(0)); nm.push_back(static_cast<UInt8>(0));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnNullable &>(*chunk.getColumns()[0]);
        const auto & nested = typeid_cast<const ColumnInt32 &>(decoded.getNestedColumn());
        EXPECT_EQ(nested.getData()[0], 100);
        EXPECT_EQ(nested.getData()[1], 200);
        EXPECT_EQ(nested.getData()[2], 300);
        const auto & null_map = typeid_cast<const ColumnUInt8 &>(decoded.getNullMapColumn());
        EXPECT_EQ(null_map.getData()[0], 0);
        EXPECT_EQ(null_map.getData()[1], 0);
        EXPECT_EQ(null_map.getData()[2], 0);
    }
}

TEST(ColumnBinary, UInt8RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeUInt8>()};
    auto col = ColumnUInt8::create();
    col->getData().push_back(static_cast<UInt8>(0));
    col->getData().push_back(static_cast<UInt8>(127));
    col->getData().push_back(static_cast<UInt8>(255));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnUInt8 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], 0);
        EXPECT_EQ(decoded.getData()[1], 127);
        EXPECT_EQ(decoded.getData()[2], 255);
    }
}

TEST(ColumnBinary, UInt64MinMaxRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeUInt64>()};
    auto col = ColumnUInt64::create();
    col->getData().push_back(0);
    col->getData().push_back(1);
    col->getData().push_back(18446744073709551615ULL);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnUInt64 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], 0ULL);
        EXPECT_EQ(decoded.getData()[1], 1ULL);
        EXPECT_EQ(decoded.getData()[2], 18446744073709551615ULL);
    }
}

TEST(ColumnBinary, Int8RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt8>()};
    auto col = ColumnInt8::create();
    col->getData().push_back(static_cast<Int8>(-128));
    col->getData().push_back(static_cast<Int8>(0));
    col->getData().push_back(static_cast<Int8>(127));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnInt8 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], -128);
        EXPECT_EQ(decoded.getData()[1], 0);
        EXPECT_EQ(decoded.getData()[2], 127);
    }
}

TEST(ColumnBinary, Int16RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt16>()};
    auto col = ColumnInt16::create();
    col->getData().push_back(static_cast<Int16>(-32768));
    col->getData().push_back(static_cast<Int16>(0));
    col->getData().push_back(static_cast<Int16>(32767));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnInt16 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], static_cast<Int16>(-32768));
        EXPECT_EQ(decoded.getData()[1], static_cast<Int16>(0));
        EXPECT_EQ(decoded.getData()[2], static_cast<Int16>(32767));
    }
}

TEST(ColumnBinary, UInt16RoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeUInt16>()};
    auto col = ColumnUInt16::create();
    col->getData().push_back(static_cast<UInt16>(0));
    col->getData().push_back(static_cast<UInt16>(32767));
    col->getData().push_back(static_cast<UInt16>(65535));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnUInt16 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], static_cast<UInt16>(0));
        EXPECT_EQ(decoded.getData()[1], static_cast<UInt16>(32767));
        EXPECT_EQ(decoded.getData()[2], static_cast<UInt16>(65535));
    }
}

TEST(ColumnBinary, Int32MinMaxRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt32>()};
    auto col = ColumnInt32::create();
    col->getData().push_back(static_cast<Int32>(-2147483648));
    col->getData().push_back(0);
    col->getData().push_back(2147483647);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnInt32 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], -2147483648);
        EXPECT_EQ(decoded.getData()[1], 0);
        EXPECT_EQ(decoded.getData()[2], 2147483647);
    }
}

// Regression: constructor receives a 0-row schema block (as in query planning),
// data is passed separately via write(). Before the fix, num_rows=0 was written
// because writePrefix() read header_->rows() instead of the actual chunk size.
TEST(ColumnBinary, SchemaOnlyHeaderRoundTrip)
{
    auto type = std::make_shared<DataTypeUInt64>();

    Block schema;
    schema.insert(ColumnWithTypeAndName{type->createColumn(), type, "col0"});

    Block data;
    {
        auto col = ColumnUInt64::create();
        col->getData().push_back(10ULL);
        col->getData().push_back(20ULL);
        col->getData().push_back(30ULL);
        data.insert(ColumnWithTypeAndName{std::move(col), type, "col0"});
    }

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(schema));
        output.write(data);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, schema, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnUInt64 &>(*chunk.getColumns()[0]);
        EXPECT_EQ(decoded.getData()[0], 10ULL);
        EXPECT_EQ(decoded.getData()[1], 20ULL);
        EXPECT_EQ(decoded.getData()[2], 30ULL);
    }
}

TEST(ColumnBinary, LargeRowCountRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeUInt64>()};
    auto col = ColumnUInt64::create();
    for (UInt64 i = 0; i < 1000; ++i)
        col->getData().push_back(i * i);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 1000u);
        const auto & decoded = typeid_cast<const ColumnUInt64 &>(*chunk.getColumns()[0]);
        for (UInt64 i = 0; i < 1000; ++i)
            EXPECT_EQ(decoded.getData()[i], i * i);
    }
}

TEST(ColumnBinary, ArrayWithEmptyElementRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    // 3 rows: [1, 2], [], [3]
    auto data_col = makeColUInt64({1, 2, 3});
    auto col = makeArrayColumn(data_col, {2, 2, 3});
    Block header;
    header.insert(ColumnWithTypeAndName{col, types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 3u);
        const auto & decoded = typeid_cast<const ColumnArray &>(*chunk.getColumns()[0]);
        const auto & arr_data = typeid_cast<const ColumnUInt64 &>(decoded.getData());
        const auto & offsets = decoded.getOffsets();
        EXPECT_EQ(offsets[0], 2u);
        EXPECT_EQ(offsets[1], 2u);
        EXPECT_EQ(offsets[2], 3u);
        EXPECT_EQ(arr_data.getData()[0], 1u);
        EXPECT_EQ(arr_data.getData()[1], 2u);
        EXPECT_EQ(arr_data.getData()[2], 3u);
    }
}

TEST(ColumnBinary, ArrayTupleRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes{
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeFloat64>()
            }))};

    // 2 rows: row 0 has 2 tuples, row 1 has 1 tuple
    // Tuple data: (idx, distance)
    auto col_idx = ColumnUInt64::create();
    col_idx->getData().push_back(0);
    col_idx->getData().push_back(1);
    col_idx->getData().push_back(0);

    auto col_dist = ColumnFloat64::create();
    col_dist->getData().push_back(1.5);
    col_dist->getData().push_back(2.5);
    col_dist->getData().push_back(3.5);

    auto col_idx_ptr = ColumnPtr(std::move(col_idx));
    auto col_dist_ptr = ColumnPtr(std::move(col_dist));
    Columns tuple_cols;
    tuple_cols.push_back(std::move(col_idx_ptr));
    tuple_cols.push_back(std::move(col_dist_ptr));
    auto tuple_col = ColumnTuple::create(tuple_cols);

    auto col = makeArrayColumn(std::move(tuple_col), {2, 3});
    Block header;
    header.insert(ColumnWithTypeAndName{col, types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    {
        ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
        auto chunk = input.read();
        ASSERT_EQ(chunk.getNumColumns(), 1u);
        ASSERT_EQ(chunk.getNumRows(), 2u);
        const auto & decoded = typeid_cast<const ColumnArray &>(*chunk.getColumns()[0]);
        const auto & offsets = decoded.getOffsets();
        EXPECT_EQ(offsets[0], 2u);
        EXPECT_EQ(offsets[1], 3u);
        const auto & tuple_data = typeid_cast<const ColumnTuple &>(decoded.getData());
        const auto & idx = typeid_cast<const ColumnUInt64 &>(tuple_data.getColumn(0));
        const auto & dist = typeid_cast<const ColumnFloat64 &>(tuple_data.getColumn(1));
        EXPECT_EQ(idx.size(), 3u);
        EXPECT_EQ(dist.size(), 3u);
        EXPECT_EQ(idx.getData()[0], 0u);
        EXPECT_EQ(idx.getData()[1], 1u);
        EXPECT_EQ(idx.getData()[2], 0u);
        EXPECT_DOUBLE_EQ(dist.getData()[0], 1.5);
        EXPECT_DOUBLE_EQ(dist.getData()[1], 2.5);
        EXPECT_DOUBLE_EQ(dist.getData()[2], 3.5);
    }
}

// ── column_binary_max_frame_size must be enforced before preallocation ───────
//
// The setting is checked both in precomputeSerializedSize (before the caller
// allocates a buffer sized from its return value, as the buffered WASM path
// does) and in consume (before actually writing). This test targets the
// precompute-time check specifically: a large row count with a tiny
// max_frame_size must throw before any oversized allocation happens.

TEST(ColumnBinary, MaxFrameSizeRejectsOversizedPrecompute)
{
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto col = ColumnString::create();
    for (int i = 0; i < 1000; ++i)
        col->insertData("0123456789", 10);
    size_t rows = col->size();
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});
    Block block_for_precompute = header;

    WriteBufferFromOwnString obuf;
    ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                    /*disable_preallocation=*/false, /*max_frame_size=*/64);
    // Exercise precomputeSerializedSize directly (not write()/consume()): this is the entry
    // point a caller preallocating from its return value uses, e.g. the buffered WASM path.
    EXPECT_THROW(output.precomputeSerializedSize(block_for_precompute, rows), DB::Exception);
}

// A frame within the configured limit must still succeed normally.

TEST(ColumnBinary, MaxFrameSizeAllowsFrameWithinLimit)
{
    DataTypes types = {std::make_shared<DataTypeUInt8>()};
    auto col = ColumnUInt8::create();
    col->getData().push_back(static_cast<UInt8>(42));
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/1024);
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    auto chunk = input.read();
    ASSERT_EQ(chunk.getNumColumns(), 1u);
    ASSERT_EQ(chunk.getNumRows(), 1u);
    const auto & decoded = typeid_cast<const ColumnUInt8 &>(*chunk.getColumns()[0]);
    EXPECT_EQ(decoded.getData()[0], 42);
}

// ── column_binary_max_frame_size = 0 must mean "no cap", not a zero-byte limit ──
//
// 0 is the setting's compatibility-fallback value for a `compatibility` setting
// pinned to a version before this setting existed (SettingsChangesHistory.cpp).
// Checking `frame_size > max_frame_size_` without special-casing 0 would reject
// every non-empty frame in that configuration, breaking every ColumnBinary
// read/write and every buffered WASM call using it. Exercise all three
// enforcement sites: output precompute, output consume/write, and input read.

TEST(ColumnBinary, MaxFrameSizeZeroMeansUnlimitedOnPrecompute)
{
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto col = ColumnString::create();
    for (int i = 0; i < 1000; ++i)
        col->insertData("0123456789", 10);
    size_t rows = col->size();
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});
    Block block_for_precompute = header;

    WriteBufferFromOwnString obuf;
    ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                    /*disable_preallocation=*/false, /*max_frame_size=*/0);
    EXPECT_NO_THROW(output.precomputeSerializedSize(block_for_precompute, rows));
}

TEST(ColumnBinary, MaxFrameSizeZeroMeansUnlimitedRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto col = ColumnString::create();
    for (int i = 0; i < 1000; ++i)
        col->insertData("0123456789", 10);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/0);
        output.write(header);
    }
    obuf.finalize();

    FormatSettings format_settings;
    format_settings.column_binary.max_frame_size = 0;
    ReadBufferFromString rb{obuf.str()};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, format_settings);
    auto chunk = input.read();
    ASSERT_EQ(chunk.getNumColumns(), 1u);
    ASSERT_EQ(chunk.getNumRows(), 1000u);
}

// ── Frame validator must reject descriptors pointing into header/descriptor metadata ──
//
// A descriptor with data_offset=0, data_size=1 leaves data_end unchanged at
// hdr_desc_size (0+1 < hdr_desc_size), so read() never reads a data section from
// the wire at all -- and without the fix, readColumnFromDesc would then silently
// decode the first byte of the frame header as the column's data instead of
// throwing. null_offset/offsets_offset have the same gap since they are never
// part of the data_end computation.

TEST(ColumnBinary, FrameValidatorRejectsDataOffsetInsideHeader)
{
    using namespace ColumnBinaryWire;

    const uint32_t num_rows = 1;
    const uint32_t num_cols = 1;
    const size_t hdr_desc_size = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::string frame(hdr_desc_size, '\0');
    ColumnBinaryWire::writeFrameHeader(reinterpret_cast<uint8_t *>(frame.data()), num_rows, num_cols);

    ColDescriptor desc{};
    desc.type = COL_FIXED64;
    desc.null_offset = 0;
    desc.offsets_offset = 0;
    desc.data_offset = 0;  // points at the frame header, not a real data section
    desc.data_size = 1;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    Block header;
    header.insert(ColumnWithTypeAndName{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "col0"});

    ReadBufferFromString rb{frame};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    EXPECT_THROW(input.read(), DB::Exception);
}

TEST(ColumnBinary, FrameValidatorRejectsNullOffsetInsideHeader)
{
    using namespace ColumnBinaryWire;

    const uint32_t num_rows = 1;
    const uint32_t num_cols = 1;
    const size_t hdr_desc_size = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint64_t data_off = hdr_desc_size;

    std::string frame(hdr_desc_size + 8, '\0');
    ColumnBinaryWire::writeFrameHeader(reinterpret_cast<uint8_t *>(frame.data()), num_rows, num_cols);

    ColDescriptor desc{};
    desc.type = COL_FIXED64 | COL_IS_NULLABLE;
    desc.null_offset = 4;  // nonzero, but points inside the frame header, not a real null map
    desc.offsets_offset = 0;
    desc.data_offset = data_off;
    desc.data_size = 8;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto nested_type = std::make_shared<DataTypeUInt64>();
    auto col = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeNullable>(nested_type), "col0"});

    ReadBufferFromString rb{frame};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    EXPECT_THROW(input.read(), DB::Exception);
}

// ── Top-level columns must stay inside their own byte region ──────────────────
//
// The writer lays top-level columns out contiguously and in descriptor order, so column
// `i`'s region ends exactly where column `i + 1`'s begins. A malformed frame that repoints
// one column's `null_offset` / `offsets_offset` / `data_offset` into a sibling column's bytes
// must be rejected as malformed rather than decoded from the wrong payload.

TEST(ColumnBinary, FrameValidatorRejectsCrossColumnNullOffset)
{
    using namespace ColumnBinaryWire;

    const uint32_t num_rows = 1;
    const uint32_t num_cols = 2;
    const size_t hdr_desc_size = FRAME_HEADER_BYTES + 2 * COL_DESC_BYTES;

    // Genuine layout: col0 = [hdr_desc_size, 100), col1 = [100, 108).
    std::string frame(108, '\0');
    ColumnBinaryWire::writeFrameHeader(reinterpret_cast<uint8_t *>(frame.data()), num_rows, num_cols);

    ColDescriptor desc0{};
    desc0.type = COL_FIXED64 | COL_IS_NULLABLE;
    desc0.null_offset = 104;  // inside col1's region instead of col0's own null map
    desc0.offsets_offset = 0;
    desc0.data_offset = hdr_desc_size + 4;
    desc0.data_size = 8;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES, &desc0, COL_DESC_BYTES);

    ColDescriptor desc1{};
    desc1.type = COL_FIXED64;
    desc1.null_offset = 0;
    desc1.offsets_offset = 0;
    desc1.data_offset = 100;
    desc1.data_size = 8;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES + COL_DESC_BYTES, &desc1, COL_DESC_BYTES);

    auto uint64_type = std::make_shared<DataTypeUInt64>();
    Block header;
    header.insert(ColumnWithTypeAndName{
        ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create()),
        std::make_shared<DataTypeNullable>(uint64_type), "col0"});
    header.insert(ColumnWithTypeAndName{ColumnUInt64::create(), uint64_type, "col1"});

    ReadBufferFromString rb{frame};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    EXPECT_THROW(input.read(), DB::Exception);
}

TEST(ColumnBinary, FrameValidatorRejectsCrossColumnDataOffset)
{
    using namespace ColumnBinaryWire;

    const uint32_t num_rows = 1;
    const uint32_t num_cols = 2;
    const size_t hdr_desc_size = FRAME_HEADER_BYTES + 2 * COL_DESC_BYTES;

    // Both descriptors point at the *second* column's payload, so col0 would decode col1's
    // bytes. Rejecting this is the ordered-layout invariant: once col0's region has been
    // stretched over [96, 104), col1's own range no longer starts at or after that region's
    // end, which is exactly what the check detects.
    std::string frame(hdr_desc_size + 16, '\0');
    ColumnBinaryWire::writeFrameHeader(reinterpret_cast<uint8_t *>(frame.data()), num_rows, num_cols);

    ColDescriptor desc{};
    desc.type = COL_FIXED64;
    desc.null_offset = 0;
    desc.offsets_offset = 0;
    desc.data_offset = hdr_desc_size + 8;
    desc.data_size = 8;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);
    std::memcpy(frame.data() + FRAME_HEADER_BYTES + COL_DESC_BYTES, &desc, COL_DESC_BYTES);

    auto uint64_type = std::make_shared<DataTypeUInt64>();
    Block header;
    header.insert(ColumnWithTypeAndName{ColumnUInt64::create(), uint64_type, "col0"});
    header.insert(ColumnWithTypeAndName{ColumnUInt64::create(), uint64_type, "col1"});

    ReadBufferFromString rb{frame};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    EXPECT_THROW(input.read(), DB::Exception);
}

TEST(ColumnBinary, FrameValidatorRejectsCrossColumnStringOffsets)
{
    using namespace ColumnBinaryWire;

    const uint32_t num_rows = 1;
    const uint32_t num_cols = 2;
    const size_t hdr_desc_size = FRAME_HEADER_BYTES + 2 * COL_DESC_BYTES;

    // col0 is an empty `String` column: its offsets array must live in its own region, which
    // here is the empty range [hdr_desc_size + 16, hdr_desc_size + 16). Point the offsets
    // array at col1's 24-byte payload instead. Every referenced byte stays inside the frame,
    // so only the per-column region check rejects this: without it, col0 happily decodes its
    // row count worth of offsets out of col1's bytes.
    const uint64_t col1_data = hdr_desc_size + 16;
    std::string frame(col1_data + 24, '\0');
    ColumnBinaryWire::writeFrameHeader(reinterpret_cast<uint8_t *>(frame.data()), num_rows, num_cols);

    ColDescriptor desc0{};
    desc0.type = COL_BYTES;
    desc0.null_offset = 0;
    desc0.offsets_offset = col1_data + 4;  // inside col1's region, not col0's own
    desc0.data_offset = col1_data;
    desc0.data_size = 0;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES, &desc0, COL_DESC_BYTES);

    ColDescriptor desc1{};
    desc1.type = COL_FIXEDN;
    desc1.null_offset = 0;
    desc1.offsets_offset = 0;
    desc1.data_offset = col1_data;
    desc1.data_size = 24;
    std::memcpy(frame.data() + FRAME_HEADER_BYTES + COL_DESC_BYTES, &desc1, COL_DESC_BYTES);

    Block header;
    header.insert(ColumnWithTypeAndName{ColumnString::create(), std::make_shared<DataTypeString>(), "col0"});
    header.insert(ColumnWithTypeAndName{
        ColumnFixedString::create(24), std::make_shared<DataTypeFixedString>(24), "col1"});

    ReadBufferFromString rb{frame};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    EXPECT_THROW(input.read(), DB::Exception);
}

// ── Zero-row ColumnConst chunks must round-trip to a genuinely empty chunk ────
//
// buildColDescriptor always stores exactly 1 value for a const column (COL_IS_CONST),
// regardless of the frame's row count, so the decoder's raw column is a 1-element
// column even when num_rows == 0. Without special-casing num_rows == 0 in
// readColumnFromDesc, that 1-element column would be returned as-is instead of an
// empty one, giving the chunk a column whose size() doesn't match its own row count.

TEST(ColumnBinary, ZeroRowConstColumnRoundTrip)
{
    DataTypes types = {std::make_shared<DataTypeInt32>()};
    auto inner_col = ColumnInt32::create();
    inner_col->insert(42);
    auto inner_ptr = ColumnPtr(std::move(inner_col));
    auto col = ColumnConst::create(inner_ptr, 0);
    Block header;
    header.insert(ColumnWithTypeAndName{std::move(col), types[0], "col0"});

    WriteBufferFromOwnString obuf;
    {
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header));
        output.write(header);
    }
    obuf.finalize();

    ReadBufferFromString rb{obuf.str()};
    ColumnBinaryInputFormat input(rb, header, RowInputFormatParams{}, FormatSettings{});
    auto chunk = input.read();
    ASSERT_EQ(chunk.getNumColumns(), 1u);
    ASSERT_EQ(chunk.getNumRows(), 0u);
    EXPECT_EQ(chunk.getColumns()[0]->size(), 0u);
}

// ── precomputeSerializedSize must model the same layout as consume ───────────
//
// consume() strips Sparse/Replicated wrappers before building descriptors, so
// precomputeSerializedSize() has to as well: buildColDescriptor has no notion of
// them, and a sparse String would miss the ColumnString branch and throw instead
// of returning a size. The two passes disagreeing would mis-size the buffer a
// caller preallocates from the precomputed value.

TEST(ColumnBinary, PrecomputeStripsSparseAndMatchesConsume)
{
    auto type = std::make_shared<DataTypeString>();

    auto values = ColumnString::create();
    values->insertDefault();          // ColumnSparse's values[0] is the default value
    values->insertData("abc", 3);
    auto offsets = ColumnUInt64::create();
    offsets->getData().push_back(3);  // only row 3 is non-default

    const size_t rows = 5;
    auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), rows);
    ASSERT_TRUE(typeid_cast<const ColumnSparse *>(sparse.get()) != nullptr);

    Block header;
    header.insert(ColumnWithTypeAndName{type->createColumn(), type, "col0"});

    Block block;
    block.insert(ColumnWithTypeAndName{std::move(sparse), type, "col0"});

    WriteBufferFromOwnString obuf;
    ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                    /*disable_preallocation=*/false, /*max_frame_size=*/0);

    std::optional<uint64_t> precomputed;
    ASSERT_NO_THROW(precomputed = output.precomputeSerializedSize(block, rows));
    ASSERT_TRUE(precomputed.has_value());

    // The size consume() actually writes must match what precompute promised.
    output.write(block);
    EXPECT_EQ(obuf.str().size(), *precomputed);
}

// ── the writer must enforce the same exact column count the reader requires ──
//
// ColumnBinaryInputFormat::checkNumCols rejects anything but an exact match
// against the schema, so the writer clamping to min(chunk columns, header
// columns) would fail open: extra columns silently dropped, missing columns
// producing a frame whose num_cols disagrees with the advertised header (and,
// on the preallocated buffered-WASM path, a precomputed size that consume does
// not fill).

TEST(ColumnBinary, WriterRejectsColumnCountMismatch)
{
    auto type = std::make_shared<DataTypeUInt64>();

    Block header;
    header.insert(ColumnWithTypeAndName{type->createColumn(), type, "col0"});
    header.insert(ColumnWithTypeAndName{type->createColumn(), type, "col1"});

    auto make_col = [&](UInt64 v)
    {
        auto c = ColumnUInt64::create();
        c->getData().push_back(v);
        return c;
    };

    // Too few columns.
    {
        Block block;
        block.insert(ColumnWithTypeAndName{make_col(1), type, "col0"});

        WriteBufferFromOwnString obuf;
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/0);
        EXPECT_THROW(output.precomputeSerializedSize(block, 1), DB::Exception);
        EXPECT_THROW(output.write(block), DB::Exception);
    }

    // Too many columns.
    {
        Block block;
        block.insert(ColumnWithTypeAndName{make_col(1), type, "col0"});
        block.insert(ColumnWithTypeAndName{make_col(2), type, "col1"});
        block.insert(ColumnWithTypeAndName{make_col(3), type, "col2"});

        WriteBufferFromOwnString obuf;
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/0);
        EXPECT_THROW(output.precomputeSerializedSize(block, 1), DB::Exception);
        EXPECT_THROW(output.write(block), DB::Exception);
    }
}

TEST(ColumnBinary, WriterRejectsColumnTypeMismatch)
{
    auto declared_type = std::make_shared<DataTypeUInt64>();
    auto other_type = std::make_shared<DataTypeString>();

    Block header;
    header.insert(ColumnWithTypeAndName{declared_type->createColumn(), declared_type, "col0"});

    // Same column count, wrong type: the reader decodes against the declared type and
    // rejects the frame (see ColumnBinaryInputFormat's structureEquals check), so the
    // writer must refuse to emit it rather than produce something unreadable.
    {
        auto c = ColumnString::create();
        c->insertData("abc", 3);

        Block block;
        block.insert(ColumnWithTypeAndName{std::move(c), other_type, "col0"});

        WriteBufferFromOwnString obuf;
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/0);
        EXPECT_THROW(output.precomputeSerializedSize(block, 1), DB::Exception);
        EXPECT_THROW(output.write(block), DB::Exception);
    }

    // A same-width but differently-typed column is still a mismatch: Int64 and UInt64 are
    // both 8 bytes, so only a structural check (not a size check) catches this.
    {
        auto int_type = std::make_shared<DataTypeInt64>();
        auto c = ColumnInt64::create();
        c->getData().push_back(-1);

        Block block;
        block.insert(ColumnWithTypeAndName{std::move(c), int_type, "col0"});

        WriteBufferFromOwnString obuf;
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/0);
        EXPECT_THROW(output.precomputeSerializedSize(block, 1), DB::Exception);
        EXPECT_THROW(output.write(block), DB::Exception);
    }

    // The matching type still writes cleanly, so the new check does not reject valid frames.
    {
        auto c = ColumnUInt64::create();
        c->getData().push_back(7);

        Block block;
        block.insert(ColumnWithTypeAndName{std::move(c), declared_type, "col0"});

        WriteBufferFromOwnString obuf;
        ColumnBinaryOutputFormat output(obuf, std::make_shared<const Block>(header),
                                        /*disable_preallocation=*/false, /*max_frame_size=*/0);
        EXPECT_NO_THROW(output.precomputeSerializedSize(block, 1));
        EXPECT_NO_THROW(output.write(block));
    }
}
