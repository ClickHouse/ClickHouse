#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/IProcessor.h>

namespace DB
{

PlanSquashingTransform::PlanSquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header)), balance(header, min_block_size_rows, min_block_size_bytes)
{
}

IProcessor::Status PlanSquashingTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        status = !has_data ? prepareConsume()
                           : prepareSend();
    }

    return status;
}

IProcessor::Status PlanSquashingTransform::prepareConsume()
{
    finished = false;
    bool all_finished = true;
    for (auto & output : outputs)
    {
        if (output.isFinished())
            continue;

        all_finished = false;
    }

    if (all_finished) /// If all outputs are closed, we close inputs (just in case)
    {
        for (auto & in : inputs)
            in.close();
        return Status::Finished;
    }

    all_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;

        all_finished = false;
    }

    if (all_finished) /// If all inputs are closed, we check if we have data in balancing
    {
        if (balance.isDataLeft()) /// If we have data in balancing, we process this data
        {
            finished = false;
            transform(chunk);
            has_data = true;
        }
        else    /// If we don't have data, We send FINISHED
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }
    }

    while (!chunk.hasChunkInfo())
    {
        for (auto & input : inputs)
        {
            if (input.isFinished())
                continue;

            input.setNeeded();
            if (!input.hasData())
            {
                if (!balance.isDataLeft())
                    return Status::NeedData;
                else
                {
                    finished = true;
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
                has_data = true;
                return Status::Ready;
            }

        }
    }
    return Status::Ready;
}

void PlanSquashingTransform::transform(Chunk & chunk_)
{
    if (!finished)
    {
        Chunk res_chunk = balance.add(std::move(chunk_));
        std::swap(res_chunk, chunk_);
    }
    else
    {
        Chunk res_chunk = balance.add({});
        std::swap(res_chunk, chunk_);
    }
}

IProcessor::Status PlanSquashingTransform::prepareSend()
{
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
