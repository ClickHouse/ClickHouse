#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <gtest/gtest.h>

using namespace DB;

/// Regression tests for issue #113475: a merge spun forever at 100% CPU when
/// `merge_max_block_size_bytes` was below the `byteSize()` of an *empty* column.

namespace
{

constexpr UInt64 MAX_BLOCK_SIZE = 256;
constexpr UInt64 MAX_BLOCK_SIZE_BYTES = 8;
constexpr UInt64 LARGE_MAX_BLOCK_SIZE_BYTES = 1024 * 1024;

/// An empty `LowCardinality` column still carries its dictionary, so it reports a
/// non-zero `byteSize()` at zero rows. That is what lets the byte limit be met
/// before a single row has been merged.
Block makeHeader()
{
    auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "a"));
    header.insert(ColumnWithTypeAndName(lc_type->createColumn(), lc_type, "b"));
    return header;
}

IMergingAlgorithm::Inputs makeInputs(const Block & header)
{
    MutableColumns source = header.cloneEmptyColumns();
    source[0]->insert(Field(UInt64(1)));
    source[1]->insert(Field(String("")));

    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk.setColumns(std::move(source), 1);
    return inputs;
}

size_t totalByteSize(const MutableColumns & columns)
{
    size_t bytes = 0;
    for (const auto & column : columns)
        bytes += column->byteSize();
    return bytes;
}

}

/// Anti-vacuity precondition: if an empty accumulator ever stopped meeting the byte
/// limit, the assertion in the next test would hold without the fix and test nothing.
TEST(MergedDataHasEnoughRows, EmptyAccumulatorAlreadyMeetsTheByteLimit)
{
    EXPECT_GE(totalByteSize(makeHeader().cloneEmptyColumns()), MAX_BLOCK_SIZE_BYTES);
}

TEST(MergedDataHasEnoughRows, ZeroRowsIsNeverEnoughBecauseOfTheByteLimit)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, inputs);

    /// Before the fix the byte branch returned true here, so the algorithm pulled a
    /// 0-row chunk without advancing its queue and was re-entered forever.
    EXPECT_EQ(merged_data.mergedRows(), 0u);
    EXPECT_FALSE(merged_data.hasEnoughRows());
}

/// Positive control: the byte limit must still fire once a row is present, so the fix
/// constrains the predicate at zero rows instead of disabling the limit.
TEST(MergedDataHasEnoughRows, ByteLimitIsStillEnforcedAfterOneRow)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, inputs);

    const auto & source_columns = inputs[0].chunk.getColumns();
    ColumnRawPtrs raw_columns{source_columns[0].get(), source_columns[1].get()};
    merged_data.insertRow(raw_columns, 0, 1);

    EXPECT_EQ(merged_data.mergedRows(), 1u);
    EXPECT_TRUE(merged_data.hasEnoughRows());
}

/// The byte limit must stay a real comparison rather than a proxy for "some row exists":
/// one row well below the threshold is not enough.
TEST(MergedDataHasEnoughRows, OneRowBelowTheByteLimitIsNotEnough)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, LARGE_MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, inputs);

    const auto & source_columns = inputs[0].chunk.getColumns();
    ColumnRawPtrs raw_columns{source_columns[0].get(), source_columns[1].get()};
    merged_data.insertRow(raw_columns, 0, 1);

    EXPECT_EQ(merged_data.mergedRows(), 1u);
    EXPECT_FALSE(merged_data.hasEnoughRows());
}

/// Control: a zero byte limit disables the branch, so it cannot report enough rows.
TEST(MergedDataHasEnoughRows, ZeroByteLimitDisablesTheBranch)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, 0, {});
    merged_data.initialize(header, inputs);

    EXPECT_FALSE(merged_data.hasEnoughRows());
}
