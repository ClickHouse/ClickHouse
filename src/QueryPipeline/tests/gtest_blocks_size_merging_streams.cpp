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

    auto transform = std::make_shared<MergingSortedTransform>(
        pipe.getSharedHeader(),
        pipe.numOutputPorts(),
        sort_description,
        /*max_block_size_rows=*/ 8192,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch,
        /*limit=*/ 0,
        /*always_read_till_end=*/ false,
        /*out_row_sources_buf=*/ nullptr,
        /*filter_column_name=*/ std::nullopt,
        /*use_average_block_sizes=*/ true);

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

    auto transform = std::make_shared<MergingSortedTransform>(
        pipe.getSharedHeader(),
        pipe.numOutputPorts(),
        sort_description,
        /*max_block_size_rows=*/ 8192,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch,
        /*limit=*/ 0,
        /*always_read_till_end=*/ false,
        /*out_row_sources_buf=*/ nullptr,
        /*filter_column_name=*/ std::nullopt,
        /*use_average_block_sizes=*/ true);

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


/// Test that a two-layer hierarchical merge (6 streams, max 3 per layer) produces
/// correctly sorted output identical to a single-node merge.
TEST(MergingSortedTest, HierarchicalMerge)
{
    constexpr size_t NUM_STREAMS = 6;
    constexpr size_t MAX_PER_LAYER = 3;
    constexpr size_t ROWS_PER_STREAM = 100;

    std::vector<std::string> key_columns{"K1"};
    auto sort_description = getSortDescription(key_columns);

    /// Build sorted input streams. Stream i contains values: i, i+NUM_STREAMS, i+2*NUM_STREAMS, ...
    auto make_pipe = [&]()
    {
        Pipes pipes;
        for (size_t s = 0; s < NUM_STREAMS; ++s)
        {
            BlocksList blocks;
            auto column = ColumnUInt64::create(ROWS_PER_STREAM, 0);
            for (size_t j = 0; j < ROWS_PER_STREAM; ++j)
                column->getElement(j) = s + j * NUM_STREAMS;
            ColumnsWithTypeAndName cols;
            cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), key_columns[0]);
            blocks.push_back(Block(cols));
            pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
        }
        return Pipe::unitePipes(std::move(pipes));
    };

    auto make_merger = [&](SharedHeader header, size_t inputs) -> std::shared_ptr<MergingSortedTransform>
    {
        return std::make_shared<MergingSortedTransform>(
            header, inputs, sort_description,
            /*max_block_size_rows=*/8192, /*max_block_size_bytes=*/0,
            /*max_dynamic_subcolumns=*/std::nullopt, SortingQueueStrategy::Batch);
    };

    auto pull_all = [](QueryPipeline & pipeline) -> std::vector<UInt64>
    {
        PullingPipelineExecutor executor(pipeline);
        std::vector<UInt64> result;
        Block block;
        while (executor.pull(block))
        {
            const auto & col = assert_cast<const ColumnUInt64 &>(*block.getByPosition(0).column);
            for (size_t i = 0; i < col.size(); ++i)
                result.push_back(col.getElement(i));
        }
        return result;
    };

    /// --- Reference: single-node merge ---
    auto pipe1 = make_pipe();
    auto header = pipe1.getSharedHeader();
    pipe1.addTransform(make_merger(header, NUM_STREAMS));
    QueryPipeline pipeline1(std::move(pipe1));
    auto single_result = pull_all(pipeline1);
    ASSERT_EQ(single_result.size(), NUM_STREAMS * ROWS_PER_STREAM);

    /// --- Hierarchical merge: layer 1 has 2 groups of 3, layer 2 merges 2 ---
    auto pipe2 = make_pipe();
    size_t groups = (NUM_STREAMS + MAX_PER_LAYER - 1) / MAX_PER_LAYER;

    pipe2.transform([&](OutputPortRawPtrs ports) -> Processors
    {
        Processors processors;
        size_t port_idx = 0;
        for (size_t g = 0; g < groups; ++g)
        {
            size_t count = (g == groups - 1) ? (NUM_STREAMS - g * MAX_PER_LAYER) : MAX_PER_LAYER;
            auto merger = make_merger(header, count);
            auto & inputs = merger->getInputs();
            auto it = inputs.begin();
            for (size_t i = 0; i < count; ++i, ++port_idx, ++it)
                connect(*ports[port_idx], *it);
            processors.push_back(std::move(merger));
        }
        return processors;
    });

    /// Layer 2: merge the 2 group outputs.
    pipe2.addTransform(make_merger(header, groups));

    QueryPipeline pipeline2(std::move(pipe2));
    auto hierarchical_result = pull_all(pipeline2);

    ASSERT_EQ(hierarchical_result.size(), single_result.size());
    EXPECT_EQ(hierarchical_result, single_result);
}
