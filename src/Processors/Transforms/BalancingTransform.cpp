#include <Processors/Transforms/BalancingTransform.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include "Processors/IProcessor.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BalancingChunksTransform::BalancingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage_, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header)), max_memory_usage(max_memory_usage_), balance(header, min_block_size_rows, min_block_size_bytes)
{
}

IProcessor::Status BalancingChunksTransform::prepare()
{
    LOG_TRACE(getLogger("balancingProcessor"), "prepare");
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
    finished = false;
    while (!chunk.hasChunkInfo())
    {
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

            if (input.isFinished() && !balance.isDataLeft())
            {
                for (auto & output : outputs)
                    output.finish();

                return Status::Finished;
            }

            input.setNeeded();
            if (!input.hasData())
            {
                finished = true;
                if (!balance.isDataLeft())
                    return Status::NeedData;
                else
                {
                    transform(chunk);
                    has_data = true;
                    return Status::Ready;
                }
            }

            chunk = input.pull();
            transform(chunk);
            was_output_processed.assign(outputs.size(), false);
            if (chunk.hasChunkInfo())
            {
                LOG_TRACE(getLogger("balancingProcessor"), "hasData");
                has_data = true;
                return Status::Ready;
            }

        }
    }
    return Status::Ready;
}

void BalancingChunksTransform::transform(Chunk & chunk_)
{
    if (!finished)
    {
        Chunk res_chunk = balance.add(getInputPorts().front().getHeader().cloneWithColumns(chunk_.detachColumns()));
        std::swap(res_chunk, chunk_);
    }
    else
    {
        Chunk res_chunk = balance.add({});
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
        {
            has_data = false;
            return Status::Ready;
        }

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
        output.push(std::move(chunk));
        was_processed = true;
        break;
    }

    if (all_outputs_processed)
    {
        has_data = false;
        return Status::Ready;
    }

    return Status::PortFull;
}
}
