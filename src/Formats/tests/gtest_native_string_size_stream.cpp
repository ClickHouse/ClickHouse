#include <gtest/gtest.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

void addColumn(Block & block, const String & name, const String & type_name, const std::vector<Field> & values)
{
    auto type = DataTypeFactory::instance().get(type_name);
    auto column = type->createColumn();
    for (const auto & value : values)
        column->insert(value);
    block.insert(ColumnWithTypeAndName(std::move(column), type, name));
}

Block makeBlockWithStrings()
{
    Block block;
    addColumn(block, "s", "String", {"", "a", "hello world", String(300, 'x')});
    addColumn(block, "arr", "Array(String)", {Array{}, Array{Field("one")}, Array{Field("two"), Field("")}, Array{Field("a"), Field("b"), Field("c")}});
    addColumn(block, "n", "Nullable(String)", {Field{}, Field("x"), Field{}, Field("yy")});
    addColumn(block, "m", "Map(String, String)",
        {Map{}, Map{Tuple{Field("k"), Field("v")}}, Map{Tuple{Field(""), Field("")}}, Map{Tuple{Field("a"), Field("1")}, Tuple{Field("b"), Field("")}}});
    addColumn(block, "t", "Tuple(String, UInt64)",
        {Tuple{Field("p"), Field(UInt64(1))}, Tuple{Field(""), Field(UInt64(2))}, Tuple{Field("qq"), Field(UInt64(3))}, Tuple{Field("r"), Field(UInt64(4))}});
    addColumn(block, "lc", "LowCardinality(String)", {Field("aa"), Field("bb"), Field("aa"), Field("")});
    return block;
}

String writeToString(const Block & block, UInt64 revision)
{
    WriteBufferFromOwnString out;
    NativeWriter writer(out, revision, std::make_shared<const Block>(block.cloneEmpty()));
    writer.write(block);
    out.finalize();
    return out.str();
}

Block roundTrip(const Block & block, UInt64 revision)
{
    auto data = writeToString(block, revision);
    ReadBufferFromString in(data);
    NativeReader reader(in, revision);
    return reader.read();
}

void assertBlocksEqual(const Block & expected, const Block & actual)
{
    ASSERT_EQ(expected.columns(), actual.columns());
    ASSERT_EQ(expected.rows(), actual.rows());
    for (size_t i = 0; i < expected.columns(); ++i)
    {
        const auto & expected_column = *expected.getByPosition(i).column;
        const auto & actual_column = *actual.getByPosition(i).column;
        for (size_t row = 0; row < expected.rows(); ++row)
        {
            Field expected_value;
            Field actual_value;
            expected_column.get(row, expected_value);
            actual_column.get(row, actual_value);
            ASSERT_EQ(expected_value, actual_value) << "column " << expected.getByPosition(i).name << ", row " << row;
        }
    }
}

}

TEST(NativeStringSizeStream, RoundTripNewRevision)
{
    auto block = makeBlockWithStrings();
    assertBlocksEqual(block, roundTrip(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION));
}

TEST(NativeStringSizeStream, RoundTripOldRevision)
{
    auto block = makeBlockWithStrings();
    assertBlocksEqual(block, roundTrip(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION - 1));
}

TEST(NativeStringSizeStream, RoundTripEmptyBlock)
{
    auto block = makeBlockWithStrings().cloneEmpty();
    auto got = roundTrip(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);
    ASSERT_EQ(got.rows(), 0u);
    ASSERT_EQ(got.columns(), block.columns());
}

TEST(NativeStringSizeStream, WireLayout)
{
    Block block;
    addColumn(block, "s", "String", {Field("ab"), Field("c"), Field("")});

    /// New revision: has_custom byte, then all sizes as UInt64 little-endian, then concatenated data.
    {
        auto data = writeToString(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);
        const char expected_tail[] = "\x00"
                                     "\x02\x00\x00\x00\x00\x00\x00\x00"
                                     "\x01\x00\x00\x00\x00\x00\x00\x00"
                                     "\x00\x00\x00\x00\x00\x00\x00\x00"
                                     "abc";
        String tail(expected_tail, sizeof(expected_tail) - 1);
        ASSERT_GE(data.size(), tail.size());
        ASSERT_EQ(data.substr(data.size() - tail.size()), tail);
    }

    /// Old revision: has_custom byte, then varint length + data per value.
    {
        auto data = writeToString(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION - 1);
        const char expected_tail[] = "\x00"
                                     "\x02"
                                     "ab"
                                     "\x01"
                                     "c"
                                     "\x00";
        String tail(expected_tail, sizeof(expected_tail) - 1);
        ASSERT_GE(data.size(), tail.size());
        ASSERT_EQ(data.substr(data.size() - tail.size()), tail);
    }
}
