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
    for (size_t i = 0; i < size_of_row_in_bytes; i += sizeof(UInt64)) {
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
        std::vector<size_t> index_granularity;
        fillIndexGranularityImpl(block1, 0, 100, false, 0, index_granularity);
        EXPECT_EQ(index_granularity.size(), 1);
        EXPECT_EQ(index_granularity[0], 100);
    }

    { /// Granule size is less than block size. Block contains multiple granules.
        std::vector<size_t> index_granularity;
        fillIndexGranularityImpl(block1, 16, 100, false, 0, index_granularity);
        EXPECT_EQ(index_granularity.size(), 5); /// First granule with 8 rows, and second with 1 row
        for (auto granule_size : index_granularity)
            EXPECT_EQ(granule_size, 2);
    }

    { /// Granule size is more than block size. Whole block (and maybe more) can be placed in single granule.

        std::vector<size_t> index_granularity;
        fillIndexGranularityImpl(block1, 512, 100, false, 0, index_granularity);
        EXPECT_EQ(index_granularity.size(), 1);
        for (auto granule_size : index_granularity)
            EXPECT_EQ(granule_size, 64);
    }

    { /// Blocks with granule size

        std::vector<size_t> index_granularity;
        fillIndexGranularityImpl(block1, 1, 1, true, 0, index_granularity);
        EXPECT_EQ(index_granularity.size(), 1);
        for (auto granule_size : index_granularity)
            EXPECT_EQ(granule_size, block1.rows());
    }

    { /// Shift in index offset
        std::vector<size_t> index_granularity;
        fillIndexGranularityImpl(block1, 16, 100, false, 6, index_granularity);
        EXPECT_EQ(index_granularity.size(), 2);
        for (auto granule_size : index_granularity)
            EXPECT_EQ(granule_size, 2);
    }
}


TEST(AdaptiveIndexGranularity, FillGranularitySequenceOfBlocks)
{
    { /// Three equal blocks
        auto block1 = getBlockWithSize(65536, 8);
        auto block2 = getBlockWithSize(65536, 8);
        auto block3 = getBlockWithSize(65536, 8);
        std::vector<size_t> index_granularity;
        for (const auto & block : {block1, block2, block3})
            fillIndexGranularityImpl(block, 1024, 0, false, 0, index_granularity);

        EXPECT_EQ(index_granularity.size(), 192); /// granules
        for (auto granule_size_in_rows : index_granularity)
            EXPECT_EQ(granule_size_in_rows, 128);
    }
    { /// Three blocks of different size
        auto block1 = getBlockWithSize(65536, 32);
        auto block2 = getBlockWithSize(32768, 32);
        auto block3 = getBlockWithSize(2048, 32);
        EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), 3136);
        std::vector<size_t> index_granularity;
        for (const auto & block : {block1, block2, block3})
            fillIndexGranularityImpl(block, 1024, 0, false, 0, index_granularity);

        EXPECT_EQ(index_granularity.size(), 98); /// granules
        for (auto granule_size_in_rows : index_granularity)
            EXPECT_EQ(granule_size_in_rows, 32);

    }
    { /// Three small blocks
        auto block1 = getBlockWithSize(2048, 32);
        auto block2 = getBlockWithSize(4096, 32);
        auto block3 = getBlockWithSize(8192, 32);

        EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), (2048 + 4096 + 8192) / 32);

        std::vector<size_t> index_granularity;
        size_t index_offset = 0;
        for (const auto & block : {block1, block2, block3})
        {
            fillIndexGranularityImpl(block, 16384, 0, false, index_offset, index_granularity);
            index_offset = index_granularity.back() - block.rows();
        }
        EXPECT_EQ(index_granularity.size(), 1); /// granules
        for (auto granule_size_in_rows : index_granularity)
            EXPECT_EQ(granule_size_in_rows, 512);
    }

}
