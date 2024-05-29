#include <Processors/QueryPlan/BufferChunksTransform.h>
#include "Common/Logger.h"

namespace DB
{

BufferChunksTransform::BufferChunksTransform(const Block & header_, size_t num_ports_, size_t max_bytes_to_buffer_, size_t limit_)
    : IProcessor(InputPorts(num_ports_, header_), OutputPorts(num_ports_, header_))
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , limit(limit_)
    , chunks(num_ports_)
    , num_processed_rows(num_ports_)
{
    for (auto & input : inputs)
        input_ports.push_back({.port = &input, .is_finished = false});

    for (auto & output : outputs)
        output_ports.push_back({.port = &output, .is_finished = false});
}

IProcessor::Status BufferChunksTransform::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!is_reading_started)
    {
        for (auto & input : inputs)
            input.setNeeded();

        is_reading_started = true;
    }

    for (const auto & idx : updated_outputs)
    {
        auto & input = input_ports[idx];
        auto & output = output_ports[idx];

        if (output.port->isFinished())
        {
            if (!output.is_finished)
            {
                output.is_finished = true;
                ++num_finished_outputs;
            }
        }
        else if (output.port->canPush())
        {
            available_outputs.push(idx);
        }
        else if (num_buffered_bytes >= max_bytes_to_buffer)
        {
            input.port->setNotNeeded();
        }
    }

    for (const auto & idx : updated_inputs)
    {
        auto & input = input_ports[idx];

        if (input.port->isFinished())
        {
            if (!input.is_finished)
            {
                input.is_finished = true;
                ++num_finished_inputs;
            }
        }
        else if (input.port->hasData() && num_buffered_bytes < max_bytes_to_buffer)
        {
            auto chunk = pullChunk(idx);
            num_buffered_bytes += chunk.bytes();
            chunks[idx].push(std::move(chunk));
        }
    }

    std::queue<UInt64> next_available_outputs;
    bool pushed_directly = false;

    while (!available_outputs.empty())
    {
        UInt64 idx = available_outputs.front();
        available_outputs.pop();

        auto & input = input_ports[idx];
        auto & output = output_ports[idx];
        chassert(output.port->canPush());

        if (!chunks[idx].empty())
        {
            auto & chunk = chunks[idx].front();
            num_buffered_bytes -= chunk.bytes();
            output.port->push(std::move(chunk));
            chunks[idx].pop();
        }
        else if (input.port->hasData())
        {
            /// Process chunk without buffering if possible.
            auto chunk = pullChunk(idx);
            output.port->push(std::move(chunk));
            pushed_directly = true;
        }
        else if (input.is_finished)
        {
            output.port->finish();
            output.is_finished = true;
            ++num_finished_outputs;
        }
        else
        {
            input.port->setNeeded();
            next_available_outputs.push(idx);
        }
    }

    available_outputs = std::move(next_available_outputs);

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (num_finished_inputs == inputs.size())
    {
        if (num_buffered_bytes == 0)
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }

        return Status::PortFull;
    }

    bool need_data = pushed_directly || num_buffered_bytes < max_bytes_to_buffer;
    return need_data ? Status::NeedData : Status::PortFull;
}

Chunk BufferChunksTransform::pullChunk(size_t input_idx)
{
    auto & input = input_ports[input_idx];
    input.port->setNeeded();

    auto chunk = input.port->pull();
    num_processed_rows[input_idx] += chunk.getNumRows();

    if (limit && num_processed_rows[input_idx] >= limit)
    {
        input.port->close();
        input.is_finished = true;
        ++num_finished_inputs;
    }

    return chunk;
}

}
