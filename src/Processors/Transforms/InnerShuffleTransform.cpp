#include <memory>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Port.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <Common/WeakHash.h>
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

InnerShuffleScatterTransformV2::InnerShuffleScatterTransformV2(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_)
    : IProcessor({header_}, {header_})
    , num_streams(num_streams_)
    , header(header_)
    , hash_columns(hash_columns_)
{}


IProcessor::Status InnerShuffleScatterTransformV2::prepare()
{
    auto & outport = outputs.front();
    auto & inport = inputs.front();
    if (outport.isFinished())
    {
        inport.close();
        return Status::Finished;
    }
    if (has_input)
    {
        return Status::Ready;
    }
    if (has_output)
    {
        if(outport.canPush())
        {
            auto empty_block = header.cloneEmpty();
            Chunk chunk(empty_block.getColumns(), 0);
            auto chunk_info = std::make_shared<InnerShuffleScatterChunkInfo>();
            chunk_info->chunks.swap(output_chunks);
            chunk.setChunkInfo(chunk_info);
            outport.push(std::move(chunk));
            has_output = false;
        }
        return Status::PortFull;
    }
    if (inport.isFinished())
    {
        outport.finish();
        return Status::Finished;
    }

    inport.setNeeded();
    if (!inport.hasData())
    {
        return Status::NeedData;
    }
    input_chunk = inport.pull(true);
    has_input = true;
    return Status::Ready;
}

void InnerShuffleScatterTransformV2::work()
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
        LOG_ERROR(&Poco::Logger::get("InnerShuffleScatterTransformV2"), "scattered_block.rows() = {}, num_rows={}, num_streams:{}", scattered_block.rows(), num_rows, num_streams);
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
    bool is_output_finished = false;
    for (auto & iter : outputs)
    {
        if (iter.isFinished())
        {
            is_output_finished = true;
            break;
        }
    }
    if (is_output_finished)
    {
        for (auto & iter : outputs)
        {
            iter.finish();
        }
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
    
    // if (!output_chunks.empty()) [[likely]]
    {
        bool has_chunks_out = false;
        bool has_pending_chunks = false;
        size_t i = 0;
        for (auto & outport : outputs)
        {
            if (outport.canPush())
            {
                if (!output_chunks[i].empty())
                {
                    has_pending_chunks = true;
                    Chunk tmp_chunk;
                    tmp_chunk.swap(output_chunks[i].front());
                    output_chunks[i].pop_front();
                    outport.push(std::move(tmp_chunk));
                    has_chunks_out = true;
                }
            }
            i += 1;
        }
        // If there is no output port available, return PortFull
        if (has_pending_chunks && !has_chunks_out)
        {
            return Status::PortFull;
        }
    }

    bool all_input_finished = true;
    for (auto & inport : inputs)
    {
        if (inport.isFinished())
            continue;
        all_input_finished = false;
        inport.setNeeded();
        if (inport.hasData())
        {
            input_chunks.emplace_back(inport.pull(true));
            has_input = true;
        }
    }
    if (all_input_finished)
    {
        for (auto & outport : outputs)
        {
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
            if (output_chunks[i].empty() || output_chunks[i].back().getNumRows() >= DEFAULT_BLOCK_SIZE)
            {
                LOG_ERROR(&Poco::Logger::get("InnerShuffleDispatchTransform"), "xxx new_chunk cols:{}", chunk.getColumns().size());
                Chunk new_chunk(chunk.getColumns(), chunk.getNumRows());
                LOG_ERROR(&Poco::Logger::get("InnerShuffleDispatchTransform"), "xxx 2 new_chunk cols:{}", new_chunk.getColumns().size());
                output_chunks[i].push_back(std::move(new_chunk));
                LOG_ERROR(&Poco::Logger::get("InnerShuffleDispatchTransform"), "xxx 1 new_chunk cols:{}", output_chunks[i].back().getColumns().size());
            }
            else
            {
                auto & last_chunk = output_chunks[i].back();
                auto src_cols = chunk.getColumns();
                auto dst_cols = last_chunk.mutateColumns();
                LOG_ERROR(&Poco::Logger::get("InnerShuffleDispatchTransform"), "xxxx src cols:{}, dst cols:{}/{}", src_cols.size(), dst_cols.size(), last_chunk.getColumns().size());
                for (size_t n = 0; n < src_cols.size(); ++n)
                {
                    LOG_ERROR(&Poco::Logger::get("InnerShuffleDispatchTransform"), "xxx src_cols[{}] = {}, dst_cols[{}]:{}", n, fmt::ptr(src_cols[n].get()), n , fmt::ptr(dst_cols[n].get()));
                    dst_cols[n]->insertRangeFrom(*src_cols[n], 0, src_cols[n]->size());
                }
                auto rows = dst_cols[0]->size();
                output_chunks[i].back().setColumns(std::move(dst_cols), rows);
            }
            i += 1;
        }
    }
    has_input = false;
}

InnerShuffleGatherTransformV2::InnerShuffleGatherTransformV2(const Block & header_, size_t input_num_)
    : IProcessor(buildMultiInputports(input_num_, header_), {header_})
{
    for (auto & port : inputs)
    {
        input_port_ptrs.emplace_back(&port);
    }
}

IProcessor::Status InnerShuffleGatherTransformV2::prepare()
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

void InnerShuffleGatherTransformV2::work()
{
    has_input = false;
    has_output = true;
}

InnerShuffleScatterTransform::InnerShuffleScatterTransform(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_)
    : IProcessor({header_}, buildMultiOutputports(num_streams_, header_))
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
                // No matter can push or not, mark as port full.
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
    : IProcessor(buildMultiInputports(inputs_num_, header_), {header_})
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
