#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wundef"
#endif
#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>

// I know that inclusion of .cpp is not good at all
#include <Storages/MergeTree/MergedBlockOutputStream.cpp>

using namespace DB;
Block getBlockWithSize(size_t required_size_in_bytes, size_t size_of_row_in_bytes)
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
        MergeTreeIndexGranularity index_granularity;
        fillIndexGranularityImpl(block1, 0, 100, false, 0, index_granularity);
        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        EXPECT_EQ(index_granularity.getMarkRows(0), 100);
    }

    { /// Granule size is less than block size. Block contains multiple granules.
        MergeTreeIndexGranularity index_granularity;
        fillIndexGranularityImpl(block1, 16, 100, false, 0, index_granularity);
        EXPECT_EQ(index_granularity.getMarksCount(), 5); /// First granule with 8 rows, and second with 1 row
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 2);
    }

    { /// Granule size is more than block size. Whole block (and maybe more) can be placed in single granule.

        MergeTreeIndexGranularity index_granularity;
        fillIndexGranularityImpl(block1, 512, 100, false, 0, index_granularity);
        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 64);
    }

    { /// Blocks with granule size

        MergeTreeIndexGranularity index_granularity;
        fillIndexGranularityImpl(block1, 1, 100, true, 0, index_granularity);
        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), block1.rows());
    }

    { /// Shift in index offset
        MergeTreeIndexGranularity index_granularity;
        fillIndexGranularityImpl(block1, 16, 100, false, 6, index_granularity);
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
        MergeTreeIndexGranularity index_granularity;
        for (const auto & block : {block1, block2, block3})
            fillIndexGranularityImpl(block, 1024, 8192, false, 0, index_granularity);

        EXPECT_EQ(index_granularity.getMarksCount(), 192); /// granules
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 128);
    }
    { /// Three blocks of different size
        auto block1 = getBlockWithSize(65536, 32);
        auto block2 = getBlockWithSize(32768, 32);
        auto block3 = getBlockWithSize(2048, 32);
        EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), 3136);
        MergeTreeIndexGranularity index_granularity;
        for (const auto & block : {block1, block2, block3})
            fillIndexGranularityImpl(block, 1024, 8192, false, 0, index_granularity);

        EXPECT_EQ(index_granularity.getMarksCount(), 98); /// granules
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 32);

    }
    { /// Three small blocks
        auto block1 = getBlockWithSize(2048, 32);
        auto block2 = getBlockWithSize(4096, 32);
        auto block3 = getBlockWithSize(8192, 32);

        EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), (2048 + 4096 + 8192) / 32);

        MergeTreeIndexGranularity index_granularity;
        size_t index_offset = 0;
        for (const auto & block : {block1, block2, block3})
        {
            fillIndexGranularityImpl(block, 16384, 8192, false, index_offset, index_granularity);
            index_offset = index_granularity.getLastMarkRows() - block.rows();
        }
        EXPECT_EQ(index_granularity.getMarksCount(), 1); /// granules
        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), 512);
    }

}

TEST(AdaptiveIndexGranularity, TestIndexGranularityClass)
{
    {
        MergeTreeIndexGranularity index_granularity;
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

        EXPECT_EQ(index_granularity.getRowsCountInRange({0, 10}), sum_rows);
        EXPECT_EQ(index_granularity.getRowsCountInRange({0, 1}), 10);
        EXPECT_EQ(index_granularity.getRowsCountInRange({2, 5}), 30 + 40 + 50);


        EXPECT_EQ(index_granularity.getRowsCountInRanges({{2, 5}, {0, 1}, {0, 10}}), 10 + 30 + 40 + 50 + sum_rows);
    }
}
