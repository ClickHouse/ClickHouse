#include <gtest/gtest.h>

#include <Columns/ColumnNullable.h>
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

Block makeBlockWithNullables()
{
    Block block;
    addColumn(block, "no_nulls", "Nullable(UInt64)", {Field(UInt64(1)), Field(UInt64(2)), Field(UInt64(3)), Field(UInt64(4))});
    addColumn(block, "all_nulls", "Nullable(UInt64)", {Field{}, Field{}, Field{}, Field{}});
    addColumn(block, "some_nulls", "Nullable(String)", {Field{}, Field("x"), Field{}, Field("yy")});
    addColumn(block, "arr", "Array(Nullable(UInt32))",
        {Array{}, Array{Field(UInt64(1)), Field{}}, Array{Field(UInt64(2))}, Array{Field{}, Field{}, Field(UInt64(3))}});
    addColumn(block, "tuple", "Tuple(Nullable(UInt8), String)",
        {Tuple{Field(UInt64(1)), Field("a")}, Tuple{Field{}, Field("b")}, Tuple{Field(UInt64(3)), Field("")}, Tuple{Field{}, Field("d")}});
    addColumn(block, "lc", "LowCardinality(Nullable(String))", {Field("aa"), Field{}, Field("aa"), Field("")});
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

TEST(NativeCompactNullMap, RoundTripNewRevision)
{
    auto block = makeBlockWithNullables();
    assertBlocksEqual(block, roundTrip(block, DBMS_MIN_REVISION_WITH_COMPACT_NULL_MAP_SERIALIZATION));
}

TEST(NativeCompactNullMap, RoundTripOldRevision)
{
    auto block = makeBlockWithNullables();
    assertBlocksEqual(block, roundTrip(block, DBMS_MIN_REVISION_WITH_COMPACT_NULL_MAP_SERIALIZATION - 1));
}

TEST(NativeCompactNullMap, RoundTripRevisionZero)
{
    auto block = makeBlockWithNullables();
    assertBlocksEqual(block, roundTrip(block, 0));
}

TEST(NativeCompactNullMap, NoNullMapDataWithoutNulls)
{
    static constexpr size_t num_rows = 10000;

    Block block;
    std::vector<Field> values;
    values.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        values.emplace_back(UInt64(i));
    addColumn(block, "x", "Nullable(UInt64)", values);

    /// The null map of a column without NULLs is not sent, it is a byte per row at the old revision.
    auto new_data = writeToString(block, DBMS_MIN_REVISION_WITH_COMPACT_NULL_MAP_SERIALIZATION);
    auto old_data = writeToString(block, DBMS_MIN_REVISION_WITH_COMPACT_NULL_MAP_SERIALIZATION - 1);
    ASSERT_GE(old_data.size(), new_data.size() + num_rows - 16);

    /// The column is read without a null map.
    ReadBufferFromString in(new_data);
    NativeReader reader(in, DBMS_MIN_REVISION_WITH_COMPACT_NULL_MAP_SERIALIZATION);
    auto result = reader.read();
    assertBlocksEqual(block, result);
    const auto & column = assert_cast<const ColumnNullable &>(*result.getByPosition(0).column);
    ASSERT_FALSE(column.hasNullMap());
}
