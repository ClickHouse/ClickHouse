#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Port.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <Common/WeakHash.h>
#include <Common/logger_useful.h>

namespace DB
{
static InputPorts buildGatherInputports(size_t num_streams, const Block & header)
{
    InputPorts ports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        InputPort port(header);
        ports.push_back(port);
    }
    return ports;
}

static OutputPorts buildScatterOutports(size_t num_streams, const Block & header)
{
    OutputPorts outports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        OutputPort outport(header);
        outports.push_back(outport);
    }
    return outports;
}

InnerShuffleScatterTransform::InnerShuffleScatterTransform(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_)
    : IProcessor({header_}, buildScatterOutports(num_streams_, header_))
    , num_streams(num_streams_)
    , header(header_)
    , hash_columns(hash_columns_)
{
    for (size_t i = 0; i < num_streams; ++i)
    {
        pending_output_chunks.emplace_back(std::list<Chunk>());
    }
}

IProcessor::Status InnerShuffleScatterTransform::prepare()
{
    bool all_output_finished = true;
    bool has_port_full = false;
    auto outport_it = outputs.begin();
    auto outchunk_it = pending_output_chunks.begin();
    for (; outchunk_it != pending_output_chunks.end();)
    {
        auto & outport = *outport_it;
        auto & part_chunks = *outchunk_it;
        if (outport.isFinished())
        {
            part_chunks.clear();
        }
        else
        {
            all_output_finished = false;
            if (!part_chunks.empty())
            {
               has_port_full = true;
               if (outport.canPush())
               {
                    outport.push(std::move(part_chunks.front()));
                    part_chunks.pop_front();
               }
            }
        }
        outport_it++;
        outchunk_it++;
    }
    if (all_output_finished)
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }
    if (has_port_full)
    {
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    auto & input = inputs.front();
    if (input.isFinished())
    {
        bool has_pending_chunks = false;
        auto outport_iter = outputs.begin();
        auto outchunk_iter = pending_output_chunks.begin();
        for (; outchunk_iter != pending_output_chunks.end();)
        {
            const auto & chunks = *outchunk_iter;
            if (chunks.empty() && !outport_iter->isFinished() && !outport_iter->hasData())
            {
                outport_iter->finish();
            }
            else
            {
                has_pending_chunks = true;
            }
            outport_iter++;
            outchunk_iter++;
        }
        if (has_pending_chunks)
            return Status::Ready;
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void InnerShuffleScatterTransform::work()
{
    if (!has_input)
    {
        return;
    }
    Block block = header.cloneWithColumns(input_chunk.detachColumns());
    size_t num_rows = block.rows();
    WeakHash32 hash(num_rows);
    for (const auto col_index : hash_columns)
    {
        const auto & key_col = block.getByPosition(col_index).column->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(recursiveRemoveSparse(key_col));
        key_col_no_lc->updateWeakHash32(hash);
    }

    IColumn::Selector selector(num_rows);
    const auto & hash_data = hash.getData();
    for (size_t i = 0; i < num_rows; ++i)
    {
        selector[i] = hash_data[i] & (num_streams - 1);
    }

    Blocks result_blocks;
    for (size_t i = 0; i < num_streams; ++i)
    {
        result_blocks.emplace_back(header.cloneEmpty());
    }

    for (size_t i = 0, num_cols = header.columns(); i < num_cols; ++i)
    {
        auto shuffled_columms = block.getByPosition(i).column->scatter(num_streams, selector);
        for (size_t block_index = 0; block_index < num_streams; ++block_index)
        {
            result_blocks[block_index].getByPosition(i).column = std::move(shuffled_columms[block_index]);
        }
    }
    for (size_t i = 0; i < result_blocks.size(); ++i)
    {
        pending_output_chunks[i].emplace_back(result_blocks[i].getColumns(), result_blocks[i].rows());
    }

    has_output = true;
    has_input = false;
}

InnerShuffleGatherTransform::InnerShuffleGatherTransform(const Block & header_, size_t inputs_num_)
    : IProcessor(buildGatherInputports(inputs_num_, header_), {header_})
{
    for (auto & port : inputs)
    {
        running_inputs.emplace_back(&port);
    }
}

IProcessor::Status InnerShuffleGatherTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto * input : running_inputs)
        {
            if (!input->isFinished())
                input->close();
        }
        return Status::Finished;
    }

    if (!output.canPush())
    {
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    bool all_inputs_closed = true;
    for (auto it = running_inputs.begin(); it != running_inputs.end();)
    {
        auto * input = *it;
        if (input->isFinished())
        {
            running_inputs.erase(it++);
            continue;
        }
        it++;
        all_inputs_closed = false;
        input->setNeeded();
        if (!input->hasData())
        {
            continue;
        }
        input_chunks.emplace_back(input->pull(true));
        input->setNeeded();
        pending_rows += input_chunks.back().getNumRows();
        pending_chunks += 1;
        has_input = true;
    }

    if (has_input)
        return Status::Ready;

    if (all_inputs_closed) [[unlikely]]
    {
        if (pending_rows)
        {
            has_input = true;
            return Status::Ready;
        }
        else
        {
            for (auto & port : outputs)
            {
                if (!port.isFinished())
                {
                    port.finish();
                }
            }
            return Status::Finished;
        }
    }

    if (!has_input)
        return Status::NeedData;
    return Status::Ready;
}

void InnerShuffleGatherTransform::work()
{
    if (has_input)
    {
        has_input = false;
        if (pending_rows >= DEFAULT_BLOCK_SIZE || running_inputs.empty() || pending_chunks > 100)
        {
            output_chunk = generateOneChunk();
            has_output = true;
        }
    }
}

Chunk InnerShuffleGatherTransform::generateOneChunk()
{
    Chunk head_chunk = std::move(input_chunks.front());
    input_chunks.pop_front();
    auto mutable_cols = head_chunk.mutateColumns();
    while (!input_chunks.empty())
    {
        if (!input_chunks.front().hasRows()) [[unlikely]]
        {
            input_chunks.pop_front();
            continue;
        }
        const auto & cols = input_chunks.front().getColumns();
        auto rows = input_chunks.front().getNumRows();
        for (size_t i = 0, n = mutable_cols.size(); i < n; ++i)
        {
            auto src_col = recursiveRemoveSparse(cols[i]);
            mutable_cols[i]->insertRangeFrom(*src_col, 0, rows);
        }
        input_chunks.pop_front();
    }
    auto chunk = Chunk(std::move(mutable_cols), pending_rows);
    input_chunks.clear();
    pending_rows = 0;
    pending_chunks = 0;
    return chunk;
}
}
