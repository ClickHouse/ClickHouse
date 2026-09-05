#include <gtest/gtest.h>

#include <cstring>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
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

    /// New revision: has_custom byte, then cumulative byte offsets as UInt64 little-endian (as-is,
    /// like Array offsets), then concatenated data. For "ab", "c", "" the offsets are 2, 3, 3.
    {
        auto data = writeToString(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);
        const char expected_tail[] = "\x00"
                                     "\x02\x00\x00\x00\x00\x00\x00\x00"
                                     "\x03\x00\x00\x00\x00\x00\x00\x00"
                                     "\x03\x00\x00\x00\x00\x00\x00\x00"
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

/// Writes a two-row `String` column at the offsets revision and replaces its offset pair with
/// `first` and `second`, then reads the result back and returns the error code of the rejection.
static int deserializeWithCorruptedOffsets(UInt64 first, UInt64 second)
{
    Block block;
    addColumn(block, "s", "String", {Field("abcdefgh"), Field("ijklmnop")});
    auto data = writeToString(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);

    /// The column tail is [UInt64 offset, UInt64 offset, 16 bytes of data].
    EXPECT_EQ(data.substr(data.size() - 16), "abcdefghijklmnop");
    char corrupted_offsets[16];
    memcpy(corrupted_offsets, &first, sizeof(first));
    memcpy(corrupted_offsets + sizeof(first), &second, sizeof(second));
    data.replace(data.size() - 32, 16, corrupted_offsets, sizeof(corrupted_offsets));

    ReadBufferFromString in(data);
    NativeReader reader(in, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION);
    try
    {
        reader.read();
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

TEST(NativeStringSizeStream, CorruptedOffsetStream)
{
    /// The offsets come from untrusted input and address the characters of the column, so they have to
    /// increase monotonically and to stay within the limit on the total size of the column.

    /// A non-monotonic pair: the second string would have a negative size.
    ASSERT_EQ(deserializeWithCorruptedOffsets(8, 4), ErrorCodes::INCORRECT_DATA);

    /// An offset far past the data, which the next one then decreases from.
    ASSERT_EQ(deserializeWithCorruptedOffsets(0xfffffffffffffff0ULL, 0x20), ErrorCodes::INCORRECT_DATA);

    /// A monotonic pair whose total is above the limit on the size of the whole column.
    ASSERT_EQ(deserializeWithCorruptedOffsets(8, (1ULL << 48) + 9), ErrorCodes::INCORRECT_DATA);
}

TEST(NativeStringSizeStream, RoundTripFlattenedDynamic)
{
    /// The flattened Dynamic representation serializes its subtypes through the same
    /// revision-dependent settings, so a flattened `String` alternative also uses the
    /// offsets layout at the new revision and round-trips.
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

TEST(NativeStringSizeStream, AllEmptyNestedString)
{
    /// Array(String) with every row [] drives SerializationArray to serialize the nested String
    /// column with an empty slice (offset == 0, limit == 0). The offsets writer must emit an empty
    /// payload there instead of underflowing `offset + limit - 1` to an out-of-bounds index.
    Block block;
    addColumn(block, "arr", "Array(String)", {Array{}, Array{}, Array{}});
    assertBlocksEqual(block, roundTrip(block, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION));

    /// The same all-empty shape one level deeper, and inside a Map, to cover nested String subcolumns.
    Block nested;
    addColumn(nested, "aa", "Array(Array(String))", {Array{}, Array{}});
    addColumn(nested, "m", "Map(String, String)", {Map{}, Map{}});
    assertBlocksEqual(nested, roundTrip(nested, DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION));
}
