#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>

// I know that inclusion of .cpp is not good at all
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.cpp> // NOLINT
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>

using namespace DB;

static Block getBlockWithSize(size_t required_size_in_bytes, size_t size_of_row_in_bytes)
{

    ColumnsWithTypeAndName cols;
    size_t rows = required_size_in_bytes / size_of_row_in_bytes;
    for (size_t i = 0; i < size_of_row_in_bytes; i += sizeof(UInt64))
    {
        auto column = ColumnUInt64::create(rows, 0);
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), "column" + std::to_string(i));
    }
    return Block(cols);
}

TEST(AdaptiveIndexGranularity, FillGranularityToyTests)
{
    auto block1 = getBlockWithSize(80, 8);
    EXPECT_EQ(block1.bytes(), 80);
    { /// Granularity bytes are not set. Take default index_granularity.
        MergeTreeIndexGranularityAdaptive index_granularity;
        auto granularity = computeIndexGranularity(block1.rows(), block1.bytes(), 0, 100, false, false);
        fillIndexGranularityImpl(index_granularity, 0, granularity, block1.rows());
        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        EXPECT_EQ(index_granularity.getMarkRows(0), 100);
    }

    { /// Granule size is less than block size. Block contains multiple granules.
        MergeTreeIndexGranularityAdaptive index_granularity;
        auto granularity = computeIndexGranularity(block1.rows(), block1.bytes(), 16, 100, false, true);
        fillIndexGranularityImpl(index_granularity, 0, granularity, block1.rows());
        EXPECT_EQ(index_granularity.getMarksCount(), 5); /// First granule with 8 rows, and second with 1 row
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 2);
    }

    { /// Granule size is more than block size. Whole block (and maybe more) can be placed in single granule.

        MergeTreeIndexGranularityAdaptive index_granularity;
        auto granularity = computeIndexGranularity(block1.rows(), block1.bytes(), 512, 100, false, true);
        fillIndexGranularityImpl(index_granularity, 0, granularity, block1.rows());
        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 64);
    }

    { /// Blocks with granule size

        MergeTreeIndexGranularityAdaptive index_granularity;
        auto granularity = computeIndexGranularity(block1.rows(), block1.bytes(), 1, 100, true, true);
        fillIndexGranularityImpl(index_granularity, 0, granularity, block1.rows());
        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), block1.rows());
    }

    { /// Shift in index offset
        MergeTreeIndexGranularityAdaptive index_granularity;
        auto granularity = computeIndexGranularity(block1.rows(), block1.bytes(), 16, 100, false, true);
        fillIndexGranularityImpl(index_granularity, 6, granularity, block1.rows());
        EXPECT_EQ(index_granularity.getMarksCount(), 2);
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 2);
    }
}


TEST(AdaptiveIndexGranularity, FillGranularitySequenceOfBlocks)
{
    { /// Three equal blocks
        auto block1 = getBlockWithSize(65536, 8);
        auto block2 = getBlockWithSize(65536, 8);
        auto block3 = getBlockWithSize(65536, 8);
        MergeTreeIndexGranularityAdaptive index_granularity;
        for (const auto & block : {block1, block2, block3})
        {
            auto granularity = computeIndexGranularity(block.rows(), block.bytes(), 1024, 8192, false, true);
            fillIndexGranularityImpl(index_granularity, 0, granularity, block.rows());
        }

        EXPECT_EQ(index_granularity.getMarksCount(), 192); /// granules
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 128);
    }
    { /// Three blocks of different size
        auto block1 = getBlockWithSize(65536, 32);
        auto block2 = getBlockWithSize(32768, 32);
        auto block3 = getBlockWithSize(2048, 32);
        EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), 3136);
        MergeTreeIndexGranularityAdaptive index_granularity;
        for (const auto & block : {block1, block2, block3})
        {
            auto granularity = computeIndexGranularity(block.rows(), block.bytes(), 1024, 8192, false, true);
            fillIndexGranularityImpl(index_granularity, 0, granularity, block.rows());
        }

        EXPECT_EQ(index_granularity.getMarksCount(), 98); /// granules
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 32);

    }
    { /// Three small blocks
        auto block1 = getBlockWithSize(2048, 32);
        auto block2 = getBlockWithSize(4096, 32);
        auto block3 = getBlockWithSize(8192, 32);

        EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), (2048 + 4096 + 8192) / 32);

        MergeTreeIndexGranularityAdaptive index_granularity;
        size_t index_offset = 0;
        for (const auto & block : {block1, block2, block3})
        {
            auto granularity = computeIndexGranularity(block.rows(), block.bytes(), 16384, 8192, false, true);
            fillIndexGranularityImpl(index_granularity, index_offset, granularity, block.rows());
            index_offset = index_granularity.getLastMarkRows() - block.rows();
        }
        EXPECT_EQ(index_granularity.getMarksCount(), 1); /// granules
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 512);
    }

}

TEST(AdaptiveIndexGranularity, TestIndexGranularityAdaptive)
{
    {
        MergeTreeIndexGranularityAdaptive index_granularity;
        size_t sum_rows = 0;
        size_t sum_marks = 0;
        for (size_t i = 10; i <= 100; i+=10)
        {
            sum_rows += i;
            sum_marks++;
            index_granularity.appendMark(i);
        }
        EXPECT_EQ(index_granularity.getMarksCount(), sum_marks);
        EXPECT_EQ(index_granularity.getTotalRows(), sum_rows);
        EXPECT_EQ(index_granularity.getLastMarkRows(), 100);
        EXPECT_EQ(index_granularity.getMarkStartingRow(0), 0);
        EXPECT_EQ(index_granularity.getMarkStartingRow(1), 10);
        EXPECT_EQ(index_granularity.getMarkStartingRow(2), 30);
        EXPECT_EQ(index_granularity.getMarkStartingRow(3), 60);

        EXPECT_EQ(index_granularity.getRowsCountInRange(0, 10), sum_rows);
        EXPECT_EQ(index_granularity.getRowsCountInRange(0, 1), 10);
        EXPECT_EQ(index_granularity.getRowsCountInRange(2, 5), 30 + 40 + 50);

        EXPECT_EQ(index_granularity.getRowsCountInRanges({{2, 5}, {0, 1}, {0, 10}}), 10 + 30 + 40 + 50 + sum_rows);
    }
}

TEST(AdaptiveIndexGranularity, TestIndexGranularityConstant)
{
    auto test = [](MergeTreeIndexGranularity & index_granularity, size_t granularity_rows)
    {
        size_t sum_marks = 10;
        size_t sum_rows = granularity_rows * sum_marks;

        for (size_t i = 0; i < 10; ++i)
            index_granularity.appendMark(granularity_rows);

        size_t new_granularity_rows = granularity_rows / 2;
        index_granularity.adjustLastMark(new_granularity_rows);
        sum_rows -= (granularity_rows - new_granularity_rows);

        index_granularity.appendMark(0);
        ++sum_marks;

        EXPECT_EQ(index_granularity.getMarksCount(), sum_marks);
        EXPECT_EQ(index_granularity.getMarksCountWithoutFinal(), sum_marks - 1);
        EXPECT_EQ(index_granularity.hasFinalMark(), true);
        EXPECT_EQ(index_granularity.getTotalRows(), sum_rows);
        EXPECT_EQ(index_granularity.getTotalRows(), sum_rows);
        EXPECT_EQ(index_granularity.getLastMarkRows(), 0);
        EXPECT_EQ(index_granularity.getLastNonFinalMarkRows(), granularity_rows / 2);

        EXPECT_EQ(index_granularity.getMarkStartingRow(0), 0);
        EXPECT_EQ(index_granularity.getMarkStartingRow(3), 30);
        EXPECT_EQ(index_granularity.getMarkStartingRow(9), 90);
        EXPECT_EQ(index_granularity.getMarkStartingRow(10), sum_rows);
        EXPECT_EQ(index_granularity.getMarkStartingRow(11), sum_rows);

        EXPECT_EQ(index_granularity.getRowsCountInRange(0, 10), sum_rows);
        EXPECT_EQ(index_granularity.getRowsCountInRange(0, 11), sum_rows);
        EXPECT_EQ(index_granularity.getRowsCountInRange(0, 1), 10);
        EXPECT_EQ(index_granularity.getRowsCountInRange(2, 5), 30);
        EXPECT_EQ(index_granularity.getRowsCountInRange(3, 9), 60);
        EXPECT_EQ(index_granularity.getRowsCountInRange(5, 10), 45);
        EXPECT_EQ(index_granularity.getRowsCountInRange(5, 11), 45);

        EXPECT_EQ(index_granularity.countMarksForRows(0, 35), 3);
        EXPECT_EQ(index_granularity.countMarksForRows(5, 29), 2);
        EXPECT_EQ(index_granularity.countMarksForRows(0, 89), 8);
        EXPECT_EQ(index_granularity.countMarksForRows(0, 90), 9);
        EXPECT_EQ(index_granularity.countMarksForRows(0, 92), 9);
        EXPECT_EQ(index_granularity.countMarksForRows(0, 95), sum_marks);
        EXPECT_EQ(index_granularity.countMarksForRows(0, 99), sum_marks);
    };

    const size_t granularity_rows = 10;

    {
        MergeTreeIndexGranularityConstant index_granularity(granularity_rows);
        test(index_granularity, granularity_rows);
    }
    {
        MergeTreeIndexGranularityAdaptive index_granularity;
        test(index_granularity, granularity_rows);
    }
}
