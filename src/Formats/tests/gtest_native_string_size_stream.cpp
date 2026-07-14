#include <gtest/gtest.h>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

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

TEST(NativeStringSizeStream, CorruptedSizeStreamWraparound)
{
    Block block;
    addColumn(block, "s", "String", {Field("abcdefgh"), Field("ijklmnop")});
    auto data = writeToString(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);

    /// The column tail is [UInt64 size, UInt64 size, 16 bytes of data]. Rewrite the sizes to
    /// [0xfffffffffffffff0, 0x20]: they sum to 0x10 with a wraparound, so a naive total matches
    /// the data that is actually present while the offset of row 0 points far past it.
    ASSERT_EQ(data.substr(data.size() - 16), "abcdefghijklmnop");
    const char corrupted_sizes[] = "\xf0\xff\xff\xff\xff\xff\xff\xff"
                                   "\x20\x00\x00\x00\x00\x00\x00\x00";
    data.replace(data.size() - 32, 16, corrupted_sizes, 16);

    ReadBufferFromString in(data);
    NativeReader reader(in, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);
    try
    {
        reader.read();
        FAIL() << "expected INCORRECT_DATA for a wrapped-around size stream";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(NativeStringSizeStream, TruncatedSizeStreamWithRowsOffset)
{
    /// rows_offset is nonzero when a seeked read skips rows inside a granule. The size stream
    /// here carries a single size, while rows_offset = 2 alone needs two: the reader must
    /// report a parse error instead of wrapping num_read_rows - rows_offset around.
    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    String stream_data("\x05\x00\x00\x00\x00\x00\x00\x00", 8);
    ReadBufferFromString istr(stream_data);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr column = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(column, /*rows_offset=*/ 2, /*limit=*/ 2, settings, state, nullptr);
        FAIL() << "expected INCORRECT_DATA for a truncated size stream";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(NativeStringSizeStream, RoundTripFlattenedDynamic)
{
    /// The flattened Dynamic representation serializes its subtypes through the same
    /// revision-dependent settings, so a flattened `String` alternative also uses the
    /// size-stream layout at the new revision and round-trips.
    Block block;
    addColumn(block, "d", "Dynamic", {Field("alpha"), Field(UInt64(7)), Field("beta"), Field("")});

    FormatSettings fmt;
    fmt.native.use_flattened_dynamic_and_json_serialization = true;

    WriteBufferFromOwnString out;
    NativeWriter writer(
        out, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION, std::make_shared<const Block>(block.cloneEmpty()), fmt);
    writer.write(block);
    out.finalize();

    ReadBufferFromString in(out.str());
    NativeReader reader(in, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);
    assertBlocksEqual(block, reader.read());
}
