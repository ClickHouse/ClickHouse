#include <gtest/gtest.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

Block getHeader()
{
    return Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x"}};
}

Chunk getChunk(const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value : values)
        column->insertValue(value);

    size_t num_rows = column->size();
    Columns columns;
    columns.push_back(std::move(column));
    return Chunk(std::move(columns), num_rows);
}

/// A source that finishes its output port in the same `prepare` call that pushes
/// the last chunk, so the consumer pulls that chunk while the port is already
/// finished. `BufferChunksTransform` (used under `MergingSortedTransform` for
/// reads in order) drains its buffer the same way. With an empty last chunk this
/// reproduced `Logical error: 'max_rows > 0'`: `IMergingTransformBase::prepare`
/// passed the empty chunk to the algorithm, which pushed an empty cursor into
/// the sorting queue.
class FinishWithLastChunkSource : public IProcessor
{
public:
    FinishWithLastChunkSource(SharedHeader header, std::vector<Chunk> chunks_)
        : IProcessor({}, {std::move(header)})
        , chunks(std::move(chunks_))
    {
    }

    String getName() const override { return "FinishWithLastChunkSource"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
            return Status::Finished;

        if (!output.canPush())
            return Status::PortFull;

        if (pos == chunks.size())
        {
            output.finish();
            return Status::Finished;
        }

        output.push(std::move(chunks[pos]));
        ++pos;

        if (pos == chunks.size())
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

private:
    std::vector<Chunk> chunks;
    size_t pos = 0;
};

Pipe getPipe(std::vector<std::vector<Chunk>> inputs)
{
    auto header = std::make_shared<const Block>(getHeader());

    Pipes pipes;
    for (auto & chunks : inputs)
        pipes.emplace_back(std::make_shared<FinishWithLastChunkSource>(header, std::move(chunks)));

    return Pipe::unitePipes(std::move(pipes));
}

size_t countMergedRows(Pipe pipe, const SortDescription & sort_description, SortingQueueStrategy strategy)
{
    auto transform = std::make_shared<MergingSortedTransform>(
        pipe.getSharedHeader(),
        pipe.numOutputPorts(),
        sort_description,
        /*max_block_size_rows=*/ 8192,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        strategy,
        /*limit=*/ 0,
        /*always_read_till_end=*/ false,
        /*out_row_sources_buf=*/ nullptr,
        /*filter_column_name=*/ std::nullopt,
        /*use_average_block_sizes=*/ false);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    pipeline.setNumThreads(1);
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    Block block;
    while (executor.pull(block))
        total_rows += block.rows();

    return total_rows;
}

}

/// A single input whose last chunk is empty: the empty cursor became the only
/// element of the batch sorting queue, and its zero-size batch failed
/// `chassert(max_rows > 0)` in `MergedData::rowsToInsertBeforeFlush`.
TEST(MergingSortedEmptyChunk, SingleInputTrailingEmptyChunk)
{
    SortDescription sort_description;
    sort_description.emplace_back("x", 1, 1);

    std::vector<std::vector<Chunk>> inputs;
    inputs.push_back({});
    inputs.back().push_back(getChunk({1, 2, 3, 4, 5}));
    inputs.back().push_back(getChunk({}));

    EXPECT_EQ(countMergedRows(getPipe(std::move(inputs)), sort_description, SortingQueueStrategy::Batch), 5u);
}

/// With another input still in the queue, pushing an empty cursor makes the heap
/// comparison read row 0 of empty sort columns.
TEST(MergingSortedEmptyChunk, TwoInputsTrailingEmptyChunk)
{
    SortDescription sort_description;
    sort_description.emplace_back("x", 1, 1);

    for (auto strategy : {SortingQueueStrategy::Batch, SortingQueueStrategy::Default})
    {
        std::vector<std::vector<Chunk>> inputs;
        inputs.push_back({});
        inputs.back().push_back(getChunk({1, 3, 5}));
        inputs.back().push_back(getChunk({}));
        inputs.push_back({});
        inputs.back().push_back(getChunk({2, 4, 6, 8, 10}));

        EXPECT_EQ(countMergedRows(getPipe(std::move(inputs)), sort_description, strategy), 8u);
    }
}
