#include <algorithm>

#include <Columns/IColumn.h>
#include <Columns/ColumnSparse.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardingTransform.h>

namespace DB
{

BufferedShardingTransform::BufferedShardingTransform(SharedHeader header, size_t num_outputs_, SelectorBuilder selector_builder_)
    : IProcessor(InputPorts{header}, OutputPorts{num_outputs_, header})
    , num_outputs(num_outputs_)
    , selector_builder(std::move(selector_builder_))
    , output_queues(num_outputs_)
    , output_columns(num_outputs_)
{
    chassert(num_outputs > 0);
}

IProcessor::Status BufferedShardingTransform::prepare()
{
    auto & input = getInputs().front();

    /// Free queues for outputs closed by downstream
    bool all_finished = true;
    auto output_it = outputs.begin();
    for (size_t out = 0; out < num_outputs; ++out, ++output_it)
    {
        if (output_it->isFinished())
            output_queues[out].clear();
        else
            all_finished = false;
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    /// Pending input chunk takes priority - split it before doing anything else.
    if (has_pending_input_chunk)
        return Status::Ready;

    /// Scan queues to decide what to do next.
    bool has_queued_chunks = false; /// any output has chunks waiting in its queue
    bool has_pushable_queued_chunks = false; /// at least one queued chunk can be pushed right now (port is ready)
    bool any_queue_at_capacity = false; /// at least one output's queue hit the back-pressure cap

    auto queued_output_it = outputs.begin();
    for (size_t out = 0; out < num_outputs; ++out, ++queued_output_it)
    {
        const auto & queue = output_queues[out];
        if (queue.size() >= MAX_QUEUE_LENGTH)
            any_queue_at_capacity = true;
        if (!queue.empty())
        {
            has_queued_chunks = true;
            if (!queued_output_it->isFinished() && queued_output_it->canPush())
                has_pushable_queued_chunks = true;
        }
    }

    /// Input exhausted - drain remaining queues, then finish.
    if (input.isFinished())
    {
        if (has_queued_chunks)
            return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;

        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    /// Cannot push any output port
    if (has_queued_chunks && !has_pushable_queued_chunks)
        return Status::PortFull;

    if (any_queue_at_capacity)
        return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;

    /// Try to pull a new input chunk.
    input.setNeeded();
    if (input.hasData())
    {
        pending_input_chunk = input.pull();
        has_pending_input_chunk = true;
        return Status::Ready;
    }

    /// No new input yet - drain what we can while waiting.
    if (has_pushable_queued_chunks)
        return Status::Ready;
    return Status::NeedData;
}

/// Split pending input chunk into per-output queues, then drain queues to output ports.
void BufferedShardingTransform::work()
{
    if (has_pending_input_chunk)
    {
        generateOutputChunks();
        has_pending_input_chunk = false;
    }

    /// Push one queued chunk per output (if the port can accept it).
    auto output_it = outputs.begin();
    for (size_t out = 0; out < num_outputs; ++out, ++output_it)
    {
        auto & queue = output_queues[out];

        if (output_it->isFinished())
        {
            queue.clear();
            continue;
        }

        if (queue.empty())
            continue;

        if (!output_it->canPush())
            continue;

        output_it->push(std::move(queue.front()));
        queue.pop_front();
    }
}

void BufferedShardingTransform::generateOutputChunks()
{
    auto columns = pending_input_chunk.detachColumns();

    chassert(!columns.empty());

    for (auto & column : columns)
        column = recursiveRemoveSparse(column);

    /// The caller-supplied selector decides the destination port of every row.
    const IColumn::Selector selector = selector_builder(columns);

    const size_t num_rows = columns.front()->size();

    const auto & chunk_infos = pending_input_chunk.getChunkInfos();

    /// Fast path: when every row goes to the same output, forward the whole chunk without the
    /// O(rows * columns) split that would otherwise copy every (possibly large) aggregate-state
    /// column. This is the common case for the residue divert — the hot set is empty, or a chunk
    /// holds only cold keys — so nothing actually needs to be split off.
    if (num_rows != 0)
    {
        const auto only_output = selector.front();
        if (std::all_of(selector.begin(), selector.end(), [only_output](auto bucket) { return bucket == only_output; }))
        {
            auto output_it = outputs.begin();
            std::advance(output_it, only_output);
            if (!output_it->isFinished())
            {
                Chunk chunk(std::move(columns), num_rows);
                chunk.setChunkInfos(chunk_infos.clone());
                output_queues[only_output].push_back(std::move(chunk));
            }
            return;
        }
    }

    /// Physically split every column into N per-output mutable columns.
    /// Skip outputs that received no rows.
    for (auto & cols : output_columns)
        cols.clear();

    for (const auto & column : columns)
    {
        auto split = column->scatter(num_outputs, selector);
        for (size_t out = 0; out < num_outputs; ++out)
            output_columns[out].push_back(std::move(split[out]));
    }

    auto output_it = outputs.begin();
    for (size_t out = 0; out < num_outputs; ++out, ++output_it)
    {
        if (output_it->isFinished())
            continue;

        const size_t out_rows = output_columns[out][0]->size();
        if (out_rows == 0)
            continue;

        Chunk chunk(std::move(output_columns[out]), out_rows);
        chunk.setChunkInfos(chunk_infos.clone());
        output_queues[out].push_back(std::move(chunk));
    }
}

}
