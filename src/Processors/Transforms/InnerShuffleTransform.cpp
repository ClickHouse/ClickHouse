#include <memory>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Port.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <Common/WeakHash.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include "InnerShuffleTransform.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
static InputPorts buildMultiInputports(size_t num_streams, const Block & header)
{
    InputPorts ports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        InputPort port(header);
        ports.push_back(port);
    }
    return ports;
}

static OutputPorts buildMultiOutputports(size_t num_streams, const Block & header)
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
    : IProcessor({header_}, {header_})
    , num_streams(num_streams_)
    , header(header_)
    , hash_columns(hash_columns_)
{}


IProcessor::Status InnerShuffleScatterTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }
    if (has_input)
    {
        return Status::Ready;
    }
    if (has_output)
    {
        if (output.canPush())
        {
            auto empty_block = header.cloneEmpty();
            Chunk chunk(empty_block.getColumns(), 0);
            auto chunk_info = std::make_shared<InnerShuffleScatterChunkInfo>();
            chunk_info->chunks.swap(output_chunks);
            chunk.setChunkInfo(chunk_info);
            output.push(std::move(chunk));
            has_output = false;
        }
        return Status::PortFull;
    }
    // There is pending chunk in output port which is not pulled out.
    if (!output.canPush())
    {
        return Status::PortFull;
    }
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
    {
        return Status::NeedData;
    }
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
    output_chunks.clear();
    for (auto & scattered_block : result_blocks)
    {
        Chunk chunk(scattered_block.getColumns(), scattered_block.rows());
        output_chunks.emplace_back(std::move(chunk));
    }
    has_output = true;
    has_input = false;
}

InnerShuffleDispatchTransform::InnerShuffleDispatchTransform(size_t input_nums_, size_t output_nums_, const Block & header_)
    : IProcessor(buildMultiInputports(input_nums_, header_), buildMultiOutputports(output_nums_, header_))
    , input_nums(input_nums_)
    , output_nums(output_nums_)
    , header(header_)
{
    for (size_t i = 0; i < output_nums; ++i)
    {
        output_chunks.emplace_back(std::list<Chunk>());
    }
}

IProcessor::Status InnerShuffleDispatchTransform::prepare()
{
    // If there is any output port is finished, make it finished.
    bool is_output_finished = true;
    for (auto & iter : outputs)
    {
        if (!iter.isFinished())
        {
            is_output_finished = false;
        }
    }
    if (is_output_finished)
    {
        for (auto & iter : inputs)
        {
            iter.close();
        }
        return Status::Finished;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    {
        bool has_chunks_out = false;
        bool has_pending_chunks = false;
        size_t i = 0;
        for (auto & outport : outputs)
        {
            if (outport.isFinished())
            {
                output_chunks[i].clear();
                i += 1;
                continue;
            }
            if (!output_chunks[i].empty())
            {
                if (outport.canPush())
                {
                    Chunk tmp_chunk;
                    tmp_chunk.swap(output_chunks[i].front());
                    output_chunks[i].pop_front();
                    outport.push(std::move(tmp_chunk));
                    has_chunks_out = true;
                }
            }
            if (!outport.canPush())
                has_pending_chunks = true;
            i += 1;
        }
        // If there is no output port available, return PortFull
        if (has_pending_chunks && !has_chunks_out)
        {
            return Status::PortFull;
        }
    }

    bool all_input_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;
        all_input_finished = false;
        input.setNeeded();
        if (input.hasData())
        {
            auto chunk = input.pull(true);
            input_chunks.emplace_back(std::move(chunk));
            has_input = true;
        }
    }
    if (all_input_finished)
    {
        for (auto & outport : outputs)
        {
            if (!outport.isFinished() && !outport.canPush())
                return Status::PortFull;
            outport.finish();
        }
        return Status::Finished;
    }
    if (!has_input)
    {
        return Status::NeedData;
    }
    return Status::Ready;
}

void InnerShuffleDispatchTransform::work()
{
    for (auto & input_chunk : input_chunks)
    {
        if (!input_chunk.hasChunkInfo())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty chunk info");
        }
        auto chunk_list = std::dynamic_pointer_cast<const InnerShuffleScatterChunkInfo>(input_chunk.getChunkInfo());
        size_t i = 0;
        for (const auto & chunk : chunk_list->chunks)
        {
            auto num_rows = chunk.getNumRows();
            if (num_rows)
            {
                if (output_chunks[i].empty() || output_chunks[i].back().getNumRows() >= DEFAULT_BLOCK_SIZE)
                {
                    Columns columns;
                    columns.reserve(chunk.getColumns().size());
                    for (const auto & col : chunk.getColumns())
                    {
                        columns.emplace_back(recursiveRemoveSparse(col));
                    }
                    Chunk new_chunk(columns, num_rows);
                    output_chunks[i].push_back(std::move(new_chunk));
                }
                else
                {
                    auto & last_chunk = output_chunks[i].back();
                    auto src_cols = chunk.getColumns();
                    auto dst_cols = last_chunk.mutateColumns();
                    for (size_t n = 0; n < src_cols.size(); ++n)
                    {
                        auto src_col = recursiveRemoveSparse(src_cols[n]);
                        dst_cols[n]->insertRangeFrom(*src_col, 0, src_col->size());
                    }
                    auto rows = dst_cols[0]->size();
                    Chunk new_chunk(std::move(dst_cols), rows);
                    output_chunks[i].back().swap(new_chunk);
                }
            }
            i += 1;
        }
    }
    input_chunks.clear();
    has_input = false;
}

InnerShuffleGatherTransform::InnerShuffleGatherTransform(const Block & header_, size_t input_num_)
    : IProcessor(buildMultiInputports(input_num_, header_), {header_})
{
    for (auto & port : inputs)
    {
        input_port_ptrs.emplace_back(&port);
    }
}

IProcessor::Status InnerShuffleGatherTransform::prepare()
{
    if (outputs.front().isFinished())
    {
        for (auto & iter : inputs)
        {
            iter.close();
        }
        return Status::Finished;
    }
    if (has_input)
    {
        return Status::Ready;
    }
    if (has_output)
    {
        if (outputs.front().canPush())
        {
            outputs.front().push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }
    if (!outputs.front().canPush())
    {
        return Status::PortFull;
    }
    size_t i = 0;
    bool all_input_finished = true;
    while (i < inputs.size())
    {
        if (!input_port_ptrs[input_port_iter]->isFinished())
        {
            all_input_finished = false;
            input_port_ptrs[input_port_iter]->setNeeded();
            if (input_port_ptrs[input_port_iter]->hasData())
            {
                output_chunk = input_port_ptrs[input_port_iter]->pull(true);
                has_input = true;
                input_port_iter = (input_port_iter + 1) % inputs.size();
                break;
            }
        }
        input_port_iter = (input_port_iter + 1) % inputs.size();
        i += 1;
    }
    if (all_input_finished)
    {
        outputs.front().finish();
        return Status::Finished;
    }
    if (!has_input)
    {
        return Status::NeedData;
    }

    return Status::Ready;
}

void InnerShuffleGatherTransform::work()
{
    has_input = false;
    has_output = output_chunk.getNumRows() > 0;
}
}
