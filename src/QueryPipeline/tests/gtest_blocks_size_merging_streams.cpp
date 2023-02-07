#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Processors/Sources/BlocksListSource.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

static Block getBlockWithSize(const std::vector<std::string> & columns, size_t rows, size_t stride, size_t & start)
{

    ColumnsWithTypeAndName cols;
    size_t size_of_row_in_bytes = columns.size() * sizeof(UInt64);
    for (size_t i = 0; i * sizeof(UInt64) < size_of_row_in_bytes; ++i)
    {
        auto column = ColumnUInt64::create(rows, 0);
        for (size_t j = 0; j < rows; ++j)
        {
            column->getElement(j) = start;
            start += stride;
        }
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), columns[i]);
    }
    return Block(cols);
}


static Pipe getInputStreams(const std::vector<std::string> & column_names, const std::vector<std::tuple<size_t, size_t, size_t>> & block_sizes)
{
    Pipes pipes;
    for (auto [block_size_in_bytes, blocks_count, stride] : block_sizes)
    {
        BlocksList blocks;
        size_t start = stride;
        while (blocks_count--)
            blocks.push_back(getBlockWithSize(column_names, block_size_in_bytes, stride, start));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
    }
    return Pipe::unitePipes(std::move(pipes));

}


static Pipe getInputStreamsEqualStride(const std::vector<std::string> & column_names, const std::vector<std::tuple<size_t, size_t, size_t>> & block_sizes)
{
    Pipes pipes;
    size_t i = 0;
    for (auto [block_size_in_bytes, blocks_count, stride] : block_sizes)
    {
        BlocksList blocks;
        size_t start = i;
        while (blocks_count--)
            blocks.push_back(getBlockWithSize(column_names, block_size_in_bytes, stride, start));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
        i++;
    }
    return Pipe::unitePipes(std::move(pipes));

}


static SortDescription getSortDescription(const std::vector<std::string> & column_names)
{
    SortDescription descr;
    for (const auto & column : column_names)
    {
        descr.emplace_back(column, 1, 1);
    }
    return descr;
}

TEST(MergingSortedTest, SimpleBlockSizeTest)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    auto pipe = getInputStreams(key_columns, {{5, 1, 1}, {10, 1, 2}, {21, 1, 3}});

    EXPECT_EQ(pipe.numOutputPorts(), 3);

    auto transform = std::make_shared<MergingSortedTransform>(pipe.getHeader(), pipe.numOutputPorts(), sort_description,
        DEFAULT_MERGE_BLOCK_SIZE, SortingQueueStrategy::Batch, 0, nullptr, false, true);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    Block block1;
    Block block2;
    executor.pull(block1);
    executor.pull(block2);

    Block tmp_block;
    ASSERT_FALSE(executor.pull(tmp_block));

    for (const auto & block : {block1, block2})
        total_rows += block.rows();

    /**
      * First block consists of 1 row from block3 with 21 rows + 2 rows from block2 with 10 rows
      * + 5 rows from block 1 with 5 rows granularity
      */
    EXPECT_EQ(block1.rows(), 8);
    /**
      * Second block consists of 8 rows from block2 + 20 rows from block3
      */
    EXPECT_EQ(block2.rows(), 28);

    EXPECT_EQ(total_rows, 5 + 10 + 21);
}


TEST(MergingSortedTest, MoreInterestingBlockSizes)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    auto pipe = getInputStreamsEqualStride(key_columns, {{1000, 1, 3}, {1500, 1, 3}, {1400, 1, 3}});

    EXPECT_EQ(pipe.numOutputPorts(), 3);

    auto transform = std::make_shared<MergingSortedTransform>(pipe.getHeader(), pipe.numOutputPorts(), sort_description,
            DEFAULT_MERGE_BLOCK_SIZE, SortingQueueStrategy::Batch, 0, nullptr, false, true);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    Block block1;
    Block block2;
    Block block3;
    executor.pull(block1);
    executor.pull(block2);
    executor.pull(block3);

    Block tmp_block;
    ASSERT_FALSE(executor.pull(tmp_block));

    EXPECT_EQ(block1.rows(), (1000 + 1500 + 1400) / 3);
    EXPECT_EQ(block2.rows(), (1000 + 1500 + 1400) / 3);
    EXPECT_EQ(block3.rows(), (1000 + 1500 + 1400) / 3);

    EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), 1000 + 1500 + 1400);
}
