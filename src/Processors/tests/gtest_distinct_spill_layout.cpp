#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Transforms/DistinctSpillLayout.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/assert_cast.h>

using namespace DB;

TEST(DistinctSpillLayout, KeepsEmittedFlagConstantThroughSorting)
{
    const auto input_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});

    for (const bool preserve_input_order : {false, true})
    {
        const DistinctSpillLayout layout(input_header, {0}, preserve_input_order);
        for (const bool suppression : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "preserve_input_order=" << preserve_input_order << ", suppression=" << suppression);
            const auto keys = suppression ? std::vector<UInt64>{3, 1, 2} : std::vector<UInt64>{3, 1, 3, 2, 1};
            auto key_column = ColumnUInt64::create();
            for (const auto key : keys)
                key_column->insertValue(key);
            MutableColumns columns;
            columns.emplace_back(std::move(key_column));
            auto chunk = suppression
                ? layout.prepareSuppressionChunk(std::move(columns))
                : layout.prepareInputChunk(Chunk(std::move(columns), keys.size()), /*first_arrival_number=*/ 0);
            const ColumnPtr initial_flag = chunk.getColumns()[layout.getFlagColumnPosition()];
            EXPECT_EQ(initial_flag->size(), keys.size());

            Block block = layout.getSpillHeader()->cloneWithColumns(chunk.detachColumns());
            if (suppression)
                sortBlock(block, layout.getKeySortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
            else
                sortBlockAndDeduplicate(block, layout.getKeySortDescription(), IColumn::PermutationSortStability::Stable);

            EXPECT_EQ(block.rows(), 3);
            const auto & sorted_flag = block.getByPosition(layout.getFlagColumnPosition()).column;
            EXPECT_EQ(sorted_flag->size(), 3);
            for (const auto & flag : {initial_flag, sorted_flag})
            {
                ASSERT_TRUE(isColumnConst(*flag));
                const auto & constant = assert_cast<const ColumnConst &>(*flag);
                EXPECT_EQ(constant.getDataColumn().size(), 1);
                EXPECT_EQ(constant.getUInt(0), suppression);
            }

            Chunks runs;
            runs.emplace_back(block.getColumns(), block.rows());
            MergeSorter sorter(layout.getSpillHeader(), std::move(runs), layout.getRunSortDescription(), 65536, 0);
            auto merged = sorter.read();
            const auto & merged_keys = assert_cast<const ColumnUInt64 &>(*merged.getColumns()[0]).getData();
            EXPECT_EQ((std::vector<UInt64>{merged_keys.begin(), merged_keys.end()}), (std::vector<UInt64>{1, 2, 3}));
            const auto & flags = assert_cast<const ColumnUInt8 &>(
                *merged.getColumns()[layout.getFlagColumnPosition()]).getData();
            ASSERT_EQ(flags.size(), 3);
            for (const auto flag : flags)
                EXPECT_EQ(flag, suppression);
        }
    }
}
