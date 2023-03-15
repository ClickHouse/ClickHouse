#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Port.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <Common/WeakHash.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
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
}

IProcessor::Status InnerShuffleScatterTransform::prepare()
{
    bool all_output_finished = true;
    for (auto & output : outputs)
    {
        if (!output.isFinished())
        {
            all_output_finished = false;
            break;
        }
    }
    if (all_output_finished)
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    for (auto & output : outputs)
    {
        if (!output.canPush())
        {
            for (auto & input : inputs)
            {
                input.setNotNeeded();
            }
            return Status::PortFull;
        }
    }

    if (has_output)
    {
        auto output_it = outputs.begin();
        auto chunk_it = output_chunks.begin();
        for (; output_it != outputs.end(); ++output_it)
        {
            if (chunk_it->getNumRows())
            {
                if (output_it->isFinished())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Output port is finished, cannot push new chunks into it");
                output_it->push(std::move(*chunk_it));
            }
            chunk_it++;
        }
        output_chunks.clear();
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        for (auto & output : outputs)
        {
            output.finish();
        }
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
    for (auto & result_block : result_blocks)
    {
        output_chunks.emplace_back(Chunk(result_block.getColumns(), result_block.rows()));
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
        for (auto * input : running_inputs)
        {
            if (!input->isFinished())
            {
                input->setNotNeeded();
            }
        }
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
        pending_rows += input_chunks.back().getNumRows();
        if (pending_rows >= DEFAULT_BLOCK_SIZE)
        {
            break;
        }
    }
    if (pending_rows >= DEFAULT_BLOCK_SIZE)
    {
        has_input = true;
        output_chunk = generateOneChunk();
    }
    else if (!all_inputs_closed)
    {
        for (auto * port : running_inputs)
        {
            if (!port->isFinished())
                port->setNeeded();
        }
        return Status::NeedData;
    }

    if (all_inputs_closed) [[unlikely]]
    {
        if (pending_rows)
        {
            has_input = true;
            output_chunk = generateOneChunk();
            return Status::Ready;
        }
        for (auto & port : outputs)
        {
            if (!port.isFinished())
            {
                port.finish();
            }
        }
        return Status::Finished;
    }

    if (!has_input)
        return Status::NeedData;
    return Status::Ready;
}

Chunk InnerShuffleGatherTransform::generateOneChunk()
{
    auto mutable_cols = input_chunks[0].mutateColumns();
    for (size_t col_index = 0, n = mutable_cols.size(); col_index < n; ++col_index)
    {
        mutable_cols[col_index]->reserve(pending_rows);
        for (size_t chunk_it = 1, m = input_chunks.size(); chunk_it < m; ++chunk_it)
        {
            const auto & src_cols = input_chunks[chunk_it].getColumns();
            mutable_cols[col_index]->insertRangeFrom(*src_cols[col_index], 0, input_chunks[chunk_it].getNumRows());
        }
    }
    auto chunk = Chunk(std::move(mutable_cols), pending_rows);
    input_chunks.clear();
    pending_rows = 0;
    return chunk;
}

void InnerShuffleGatherTransform::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}

}
