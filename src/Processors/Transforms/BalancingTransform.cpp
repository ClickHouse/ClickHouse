#include <memory>
#include <Processors/Transforms/BalancingTransform.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Interpreters/SquashingTransform.h"
#include "Processors/Chunk.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

LBalancingChunksTransform::LBalancingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage_, [[maybe_unused]] bool skip_empty_chunks_)
    : ISimpleTransform(header, header, false), max_memory_usage(max_memory_usage_), squashing(min_block_size_rows, min_block_size_bytes), balance(header, min_block_size_rows, min_block_size_bytes)
{
}

void LBalancingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        Chunk res_chunk = balance.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
        if (res_chunk.hasChunkInfo())
        {
            // std::cerr << "BalancingTransform: adding chunk " << std::endl;

            // {
            //     [[maybe_unused]]const auto * agg_info = typeid_cast<const ChunksToSquash *>(res_chunk.getChunkInfo().get());
            //     std::cerr << "End of BalancingTransform: size of one group: " << agg_info->chunks.size() << std::endl;
            //     if (!agg_info->chunks.empty())
            //         std::cerr << "!group is not empty, first column: " << agg_info->chunks[0].dumpStructure() << std::endl << std::endl;
            // }

        }
        else
            LOG_TRACE(getLogger("balancing"), "{}, BalancingTransform: not adding chunk, not finished.", reinterpret_cast<void*>(this));/// ISSUE: it's not clear why finished label is not set
        std::swap(res_chunk, chunk);
    }
    else
    {
        Chunk res_chunk = balance.add({});
        if (res_chunk.hasChunkInfo())
        {
            // std::cerr << "BalancingTransform: finished adding, NumRows:" << res_chunk.getNumRows() << ", HasInfo: " << res_chunk.hasChunkInfo() << std::endl;
            // {
            //     [[maybe_unused]]const auto * agg_info = typeid_cast<const ChunksToSquash *>(res_chunk.getChunkInfo().get());
            //     std::cerr << "End of BalancingTransform: size of one group: " << agg_info->chunks.size() << std::endl;
            //     if (!agg_info->chunks.empty())
            //         std::cerr << "!group is not empty, first column: " << agg_info->chunks[0].dumpStructure() << std::endl << std::endl;
            // }
            
        }
        else
            LOG_TRACE(getLogger("balancing"), "{}, BalancingTransform: not adding chunk on finished", reinterpret_cast<void*>(this));
        std::swap(res_chunk, chunk);
    }
    LOG_TRACE(getLogger("balancing"), "{}, BalancingTransform: struct of output chunk: {}", reinterpret_cast<void*>(this), chunk.dumpStructure());
}

IProcessor::Status LBalancingChunksTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        finished = true;
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}


BalancingChunksTransform::BalancingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage_, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header)), max_memory_usage(max_memory_usage_), squashing(min_block_size_rows, min_block_size_bytes), balance(header, min_block_size_rows, min_block_size_bytes)
{
}

IProcessor::Status BalancingChunksTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        status = !has_data ? prepareConsume()
                           : prepareSend();
    }

    return status;
}

IProcessor::Status BalancingChunksTransform::prepareConsume()
{
    LOG_TRACE(getLogger("balancingProcessor"), "prepareConsume");
    for (auto & input : inputs)
    {
        bool all_finished = true;
        for (auto & output : outputs)
        {
            if (output.isFinished())
                continue;

            all_finished = false;
        }

        if (all_finished)
        {
            input.close();
            return Status::Finished;
        }

        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull();
        was_output_processed.assign(outputs.size(), false);
        transform(chunk);
        if (chunk.hasChunkInfo())
        {
            LOG_TRACE(getLogger("balancingProcessor"), "hasData");
            has_data = true;
        }
        else
        {
            finished = true;
            LOG_TRACE(getLogger("balancingProcessor"), "hasData, finished");
            transform(chunk);
            has_data = true;
        }
    }
    return Status::Ready;
}

void BalancingChunksTransform::transform(Chunk & chunk_)
{
    if (!finished)
    {
        Chunk res_chunk = balance.add(getInputPorts().front().getHeader().cloneWithColumns(chunk_.detachColumns()));
        if (res_chunk.hasChunkInfo())
        {
            // std::cerr << "BalancingTransform: adding chunk " << std::endl;

            // {
            //     [[maybe_unused]]const auto * agg_info = typeid_cast<const ChunksToSquash *>(res_chunk.getChunkInfo().get());
            //     std::cerr << "End of BalancingTransform: size of one group: " << agg_info->chunks.size() << std::endl;
            //     if (!agg_info->chunks.empty())
            //         std::cerr << "!group is not empty, first column: " << agg_info->chunks[0].dumpStructure() << std::endl << std::endl;
            // }
        }
        else
            LOG_TRACE(getLogger("balancing"), "{}, BalancingTransform: not adding chunk, not finished.", reinterpret_cast<void*>(this));/// ISSUE: it's not clear why finished label is not set
        std::swap(res_chunk, chunk_);
    }
    else
    {
        Chunk res_chunk = balance.add({});
        if (res_chunk.hasChunkInfo())
        {
            // std::cerr << "BalancingTransform: finished adding, NumRows:" << res_chunk.getNumRows() << ", HasInfo: " << res_chunk.hasChunkInfo() << std::endl;
            // {
            //     [[maybe_unused]]const auto * agg_info = typeid_cast<const ChunksToSquash *>(res_chunk.getChunkInfo().get());
            //     std::cerr << "End of BalancingTransform: size of one group: " << agg_info->chunks.size() << std::endl;
            //     if (!agg_info->chunks.empty())
            //         std::cerr << "!group is not empty, first column: " << agg_info->chunks[0].dumpStructure() << std::endl << std::endl;
            // }
        }
        else
            LOG_TRACE(getLogger("balancing"), "{}, BalancingTransform: not adding chunk on finished", reinterpret_cast<void*>(this));
        std::swap(res_chunk, chunk_);
    }
    LOG_TRACE(getLogger("balancing"), "{}, BalancingTransform: struct of output chunk: {}, hasInfo: {}", reinterpret_cast<void*>(this), chunk_.dumpStructure(), chunk.hasChunkInfo());
}

IProcessor::Status BalancingChunksTransform::prepareSend()
{
    LOG_TRACE(getLogger("balancingProcessor"), "prepareGenerate {}", chunk.dumpStructure());
    bool all_outputs_processed = true;

    size_t chunk_number = 0;
    for (auto &output : outputs)
    {
        auto & was_processed = was_output_processed[chunk_number];
        ++chunk_number;

        if (!chunk.hasChunkInfo())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info must be not empty in prepareGenerate()");

        if (was_processed)
            continue;

        if (output.isFinished())
            continue;

        if (!output.canPush())
        {
            all_outputs_processed = false;
            continue;
        }

        LOG_TRACE(getLogger("balancingProcessor"), "chunk struct: {}", chunk.dumpStructure());
        output.push(chunk.clone());
        was_processed = true;
    }

    if (all_outputs_processed)
    {
        has_data = false;
        return Status::Ready;
    }

    return Status::PortFull;
}
}
