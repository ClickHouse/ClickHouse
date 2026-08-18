#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Processors/Sources/BlocksListSource.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

static Block getBlockWithSize(const std::vector<std::string> & columns, size_t rows)
{
    ColumnsWithTypeAndName cols;
    for (const auto & column : columns)
    {
        auto column_ptr = ColumnUInt64::create(rows, 0);
        for (size_t j = 0; j < rows; ++j)
            column_ptr->getElement(j) = j;
        cols.emplace_back(std::move(column_ptr), std::make_shared<DataTypeUInt64>(), column);
    }
    return Block(cols);
}

static Pipe getInputStreams(const std::vector<std::string> & column_names, size_t num_streams, size_t rows_per_stream)
{
    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        BlocksList blocks;
        blocks.push_back(getBlockWithSize(column_names, rows_per_stream));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
    }
    return Pipe::unitePipes(std::move(pipes));
}

static void testSplitResizeTransform(size_t instreams, size_t outstreams, size_t min_outstreams_per_resize_after_split, bool strict)
{
    constexpr size_t rows_per_stream = 10;

    const std::vector<std::string> key_columns{"K1", "K2", "K3"};

    size_t expected_groups = std::min<size_t>(instreams, outstreams / min_outstreams_per_resize_after_split);
    size_t groups_with_extra_instream = instreams % expected_groups;
    size_t groups_with_extra_outstream = outstreams % expected_groups;

    auto pipe = getInputStreams(key_columns, instreams, rows_per_stream);

    EXPECT_EQ(pipe.numOutputPorts(), instreams);
    pipe.resize(outstreams, strict, min_outstreams_per_resize_after_split);
    EXPECT_EQ(pipe.numOutputPorts(), outstreams);

    const Processors & procs = pipe.getProcessors();
    Processors resize_procs;
    for (const auto & proc : procs)
    {
        if (!strict && proc->getName() == "Resize")
            resize_procs.push_back(proc);
        else if (strict && proc->getName() == "StrictResize")
            resize_procs.push_back(proc);
    }

    ASSERT_GE(resize_procs.size(), 1);
    EXPECT_EQ(resize_procs.size(), expected_groups);

    size_t instreams_per_group = (instreams + expected_groups - 1) / expected_groups;
    size_t outstreams_per_group = (outstreams + expected_groups - 1) / expected_groups;

    size_t null_source_count = 0;
    size_t null_sink_count = 0;
    for (auto i = 0; i < resize_procs.size(); ++i)
    {
        const auto & resize = resize_procs[i];
        EXPECT_EQ(resize->getInputs().size(), instreams_per_group);
        EXPECT_EQ(resize->getOutputs().size(), outstreams_per_group);
        if (groups_with_extra_instream != 0 && i >= groups_with_extra_instream)
        {
            const auto & null_source = resize->getInputs().back().getOutputPort().getProcessor();
            EXPECT_EQ(null_source.getName(), "NullSource");
            ++null_source_count;
        }
        if (groups_with_extra_outstream != 0 && i >= groups_with_extra_outstream)
        {
            const auto & null_sink = resize->getOutputs().back().getInputPort().getProcessor();
            EXPECT_EQ(null_sink.getName(), "NullSink");
            ++null_sink_count;
        }
    }

    EXPECT_EQ(null_source_count, expected_groups * instreams_per_group - instreams);
    EXPECT_EQ(null_sink_count, expected_groups * outstreams_per_group - outstreams);

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    Block block;
    while (executor.pull(block))
    {
        total_rows += block.rows();
    }
    EXPECT_EQ(total_rows, rows_per_stream * instreams);
}


TEST(ResizeTransformTest, SplitResizeTest)
{
    for (size_t instreams = 2; instreams < 100; ++instreams)
    {
        for (size_t outstreams = 1; outstreams < 100; ++outstreams)
        {
            for (size_t min_outstreams_per_resize_after_split : {16, 24, 32})
            {
                if (outstreams / min_outstreams_per_resize_after_split > 1)
                {
                    testSplitResizeTransform(instreams, outstreams, min_outstreams_per_resize_after_split, false /* strict */);
                    testSplitResizeTransform(instreams, outstreams, min_outstreams_per_resize_after_split, true /* strict */);
                }
            }
        }
    }
}
