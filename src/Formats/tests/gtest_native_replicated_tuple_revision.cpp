#include <gtest/gtest.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
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

/// A Tuple(String, UInt64) block. When `replicated` the String tuple child is a lazily replicated
/// ColumnReplicated (as a JOIN leaves it); otherwise it is the equivalent fully materialized column.
/// Both carry the same logical values: ("a",10) ("a",20) ("a",30) ("bb",40) ("bb",50).
Block makeTupleBlock(bool replicated)
{
    auto string_type = DataTypeFactory::instance().get("String");
    auto type = DataTypeFactory::instance().get("Tuple(String, UInt64)");

    ColumnPtr string_column;
    if (replicated)
    {
        auto nested = ColumnString::create();
        nested->insert(Field(String("a")));
        nested->insert(Field(String("bb")));
        /// index 0 for the first 3 rows, index 1 for the last 2 rows.
        IColumn::Offsets offsets;
        offsets.push_back(3);
        offsets.push_back(5);
        string_column = ColumnReplicated::create(ColumnPtr(std::move(nested)), convertOffsetsToIndexes(offsets));
        EXPECT_TRUE(string_column->isReplicated());
    }
    else
    {
        auto dense = ColumnString::create();
        for (const auto & value : {"a", "a", "a", "bb", "bb"})
            dense->insert(Field(String(value)));
        string_column = std::move(dense);
    }

    auto numbers = ColumnUInt64::create();
    for (UInt64 value : {10, 20, 30, 40, 50})
        numbers->insert(Field(value));

    auto tuple = ColumnTuple::create(Columns{string_column, std::move(numbers)});

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(tuple), type, "t"));
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

/// Below DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION a peer cannot decode a REPLICATED kind stack.
/// getSerializationInfo appends a REPLICATED kind for a replicated tuple child, and a top-level
/// convertToFullColumnIfReplicated would not strip that child, so the writer would emit a nested
/// REPLICATED kind an older peer cannot read. The writer must fully densify instead: the bytes for the
/// replicated-child tuple must be identical to the equivalent dense tuple, and read back to the values.
TEST(NativeReplicatedTuple, DensifiedBelowReplicatedRevision)
{
    const UInt64 revision = DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION - 1;

    Block dense = makeTupleBlock(/*replicated=*/false);
    Block replicated = makeTupleBlock(/*replicated=*/true);

    ASSERT_EQ(writeToString(replicated, revision), writeToString(dense, revision));
    assertBlocksEqual(dense, roundTrip(replicated, revision));
}

/// At and above the revision the wire may carry REPLICATED, so the writer keeps the lazy form
/// (the streams differ from the dense one) and it still round-trips to the same values.
TEST(NativeReplicatedTuple, PreservedAtReplicatedRevision)
{
    const UInt64 revision = DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION;

    Block dense = makeTupleBlock(/*replicated=*/false);
    Block replicated = makeTupleBlock(/*replicated=*/true);

    ASSERT_NE(writeToString(replicated, revision), writeToString(dense, revision));
    assertBlocksEqual(dense, roundTrip(replicated, revision));
}
