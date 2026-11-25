#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

namespace
{

ColumnWithTypeAndName makeColumn(const String & name, size_t num_rows)
{
    return {ColumnUInt8::create(num_rows, UInt8{0}), std::make_shared<DataTypeUInt8>(), name};
}

}

/// A block with no columns reports the number of rows it was given, and it is not the absence of data.
TEST(BlockRowsWithoutColumns, RowsAreReported)
{
    Block block;
    ASSERT_EQ(block.rows(), 0);
    ASSERT_TRUE(block.hasNoColumnsAndNoRows());

    block.setRowsWithoutColumns(42);
    ASSERT_TRUE(block.empty());
    ASSERT_EQ(block.rows(), 42);
    ASSERT_FALSE(block.hasNoColumnsAndNoRows());

    ASSERT_EQ(block.cloneWithCutColumns(40, 5).rows(), 2);
    ASSERT_EQ(concatenateBlocks({block, block}).rows(), 84);
    ASSERT_EQ(block.cloneEmpty().rows(), 0);
}

/// The number of rows goes with the block: a copy keeps it, and a block that was moved from has no rows.
TEST(BlockRowsWithoutColumns, CopyAndMove)
{
    Block block;
    block.setRowsWithoutColumns(42);

    Block copy = block;
    ASSERT_EQ(copy.rows(), 42);
    ASSERT_EQ(block.rows(), 42);

    Block moved = std::move(block);
    ASSERT_EQ(moved.rows(), 42);
    // NOLINTNEXTLINE(bugprone-use-after-move,hicpp-invalid-access-moved)
    ASSERT_TRUE(block.hasNoColumnsAndNoRows());

    Block assigned;
    assigned = std::move(moved);
    ASSERT_EQ(assigned.rows(), 42);
    // NOLINTNEXTLINE(bugprone-use-after-move,hicpp-invalid-access-moved)
    ASSERT_TRUE(moved.hasNoColumnsAndNoRows());

    Block swapped;
    swapped.swap(assigned);
    ASSERT_EQ(swapped.rows(), 42);
    ASSERT_TRUE(assigned.hasNoColumnsAndNoRows());

    swapped.clear();
    ASSERT_TRUE(swapped.hasNoColumnsAndNoRows());
}

/// A block with columns takes its rows from them: the number given to it before does not come back
/// when the columns are removed.
TEST(BlockRowsWithoutColumns, ColumnsTakeOver)
{
    Block block;
    block.setRowsWithoutColumns(42);

    block.insert(makeColumn("x", 3));
    ASSERT_EQ(block.rows(), 3);

    block.erase("x");
    ASSERT_TRUE(block.hasNoColumnsAndNoRows());
}
