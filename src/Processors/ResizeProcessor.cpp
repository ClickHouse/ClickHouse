#include <Processors/ResizeProcessor.h>
#include <Interpreters/Squashing.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ResizeProcessor::Status ResizeProcessor::prepare()
{
    bool is_first_output = true;
    auto output_end = current_output;

    bool all_outs_full_or_unneeded = true;
    bool all_outs_finished = true;

    bool is_first_input = true;
    auto input_end = current_input;

    bool all_inputs_finished = true;

    auto is_end_input = [&]() { return !is_first_input && current_input == input_end; };
    auto is_end_output = [&]() { return !is_first_output && current_output == output_end; };

    auto inc_current_input = [&]()
    {
        is_first_input = false;
        ++current_input;

        if (current_input == inputs.end())
            current_input = inputs.begin();
    };

    auto inc_current_output = [&]()
    {
        is_first_output = false;
        ++current_output;

        if (current_output == outputs.end())
            current_output = outputs.begin();
    };

    /// Find next output where can push.
    auto get_next_out = [&, this]() -> OutputPorts::iterator
    {
        while (!is_end_output())
        {
            if (!current_output->isFinished())
            {
                all_outs_finished = false;

                if (current_output->canPush())
                {
                    all_outs_full_or_unneeded = false;
                    auto res_output = current_output;
                    inc_current_output();
                    return res_output;
                }
            }

            inc_current_output();
        }

        return outputs.end();
    };

    /// Find next input from where can pull.
    auto get_next_input = [&, this]() -> InputPorts::iterator
    {
        while (!is_end_input())
        {
            if (!current_input->isFinished())
            {
                all_inputs_finished = false;

                current_input->setNeeded();
                if (current_input->hasData())
                {
                    auto res_input = current_input;
                    inc_current_input();
                    return res_input;
                }
            }

            inc_current_input();
        }

        return inputs.end();
    };

    auto get_status_if_no_outputs = [&]() -> Status
    {
        if (all_outs_finished)
        {
            for (auto & in : inputs)
                in.close();

            return Status::Finished;
        }

        if (all_outs_full_or_unneeded)
        {
            for (auto & in : inputs)
                in.setNotNeeded();

            return Status::PortFull;
        }

        /// Now, we pushed to output, and it must be full.
        return Status::PortFull;
    };

    auto get_status_if_no_inputs = [&]() -> Status
    {
        if (all_inputs_finished)
        {
            for (auto & out : outputs)
                out.finish();

            return Status::Finished;
        }

        return Status::NeedData;
    };

    /// Set all inputs needed in order to evenly process them.
    /// Otherwise, in case num_outputs < num_inputs and chunks are consumed faster than produced,
    ///   some inputs can be skipped.
//    auto set_all_unprocessed_inputs_needed = [&]()
//    {
//        for (; cur_input != inputs.end(); ++cur_input)
//            if (!cur_input->isFinished())
//                cur_input->setNeeded();
//    };

    while (!is_end_input() && !is_end_output())
    {
        auto output = get_next_out();

        if (output == outputs.end())
            return get_status_if_no_outputs();

        auto input = get_next_input();

        if (input == inputs.end())
            return get_status_if_no_inputs();

        output->push(input->pull());
    }

    if (is_end_input())
        return get_status_if_no_outputs();

    /// cur_input == inputs_end()
    return get_status_if_no_inputs();
}

IProcessor::Status ResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive});

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(output_number);
            }
        }
    }

    if (!is_reading_started && !waiting_outputs.empty())
    {
        for (auto & input : inputs)
            input.setNeeded();
        is_reading_started = true;
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    for (const auto & input_number : updated_inputs)
    {
        auto & input = input_ports[input_number];
        if (input.port->isFinished())
        {
            if (input.status != InputStatus::Finished)
            {
                input.status = InputStatus::Finished;
                ++num_finished_inputs;
            }
            continue;
        }

        if (input.port->hasData())
        {
            if (input.status != InputStatus::HasData)
            {
                input.status = InputStatus::HasData;
                inputs_with_data.push(input_number);
            }
        }
    }

    while (!waiting_outputs.empty() && !inputs_with_data.empty())
    {
        auto & waiting_output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        auto & input_with_data = input_ports[inputs_with_data.front()];
        inputs_with_data.pop();

        waiting_output.port->pushData(input_with_data.port->pullData());
        input_with_data.status = InputStatus::NotActive;
        waiting_output.status = OutputStatus::NotActive;

        if (input_with_data.port->isFinished())
        {
            input_with_data.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    if (!waiting_outputs.empty())
        return Status::NeedData;

    return Status::PortFull;
}

IProcessor::Status StrictResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive, .waiting_output = -1});

        for (UInt64 i = 0; i < input_ports.size(); ++i)
            disabled_input_ports.push(i);

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(output_number);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    std::queue<UInt64> inputs_with_data;

    for (const auto & input_number : updated_inputs)
    {
        auto & input = input_ports[input_number];
        if (input.port->isFinished())
        {
            if (input.status != InputStatus::Finished)
            {
                input.status = InputStatus::Finished;
                ++num_finished_inputs;

                waiting_outputs.push(input.waiting_output);
            }
            continue;
        }

        if (input.port->hasData())
        {
            if (input.status != InputStatus::NotActive)
            {
                input.status = InputStatus::NotActive;
                inputs_with_data.push(input_number);
            }
        }
    }

    while (!inputs_with_data.empty())
    {
        auto input_number = inputs_with_data.front();
        auto & input_with_data = input_ports[input_number];
        inputs_with_data.pop();

        if (input_with_data.waiting_output == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No associated output for input with data");

        auto & waiting_output = output_ports[input_with_data.waiting_output];

        if (waiting_output.status == OutputStatus::NotActive)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status NotActive for associated output");

        if (waiting_output.status != OutputStatus::Finished)
        {
            waiting_output.port->pushData(input_with_data.port->pullData(/* set_not_needed = */ true));
            waiting_output.status = OutputStatus::NotActive;
        }
        else
            abandoned_chunks.emplace_back(input_with_data.port->pullData(/* set_not_needed = */ true));

        if (input_with_data.port->isFinished())
        {
            input_with_data.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
        else
            disabled_input_ports.push(input_number);
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// Process abandoned chunks if any.
    while (!abandoned_chunks.empty() && !waiting_outputs.empty())
    {
        auto & waiting_output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        waiting_output.port->pushData(std::move(abandoned_chunks.back()));
        abandoned_chunks.pop_back();

        waiting_output.status = OutputStatus::NotActive;
    }

    /// Enable more inputs if needed.
    while (!disabled_input_ports.empty() && !waiting_outputs.empty())
    {
        auto & input = input_ports[disabled_input_ports.front()];
        disabled_input_ports.pop();

        input.port->setNeeded();
        input.status = InputStatus::NeedData;
        input.waiting_output = waiting_outputs.front();

        waiting_outputs.pop();
    }

    /// Close all other waiting for data outputs (there is no corresponding input for them).
    while (!waiting_outputs.empty())
    {
        auto & output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        if (output.status != OutputStatus::Finished)
           ++num_finished_outputs;

        output.status = OutputStatus::Finished;
        output.port->finish();
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (disabled_input_ports.empty())
        return Status::NeedData;

    return Status::PortFull;
}

static Int64 getFreeMemory()
{
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
    {
        if (auto * mem_tracker = memory_tracker_child->getParent())
        {
            Int64 used_memory = mem_tracker->get();
            Int64 soft_limit = mem_tracker->getSoftLimit();

            if (soft_limit == 0)  // No limit is set
                return std::numeric_limits<Int64>::max();

            // If used > soft_limit, free_memory is 0 or negative
            if (used_memory < soft_limit)
                return soft_limit - used_memory;
            else
                return 0;
        }
    }
    return std::numeric_limits<Int64>::max();
}

/// This function calculates how many output ports we want to keep "active"
/// based on how much free memory is available.
/// It's just one example of a "desired concurrency" formula.
static size_t calculateDesiredActiveOutputs(
    Int64 free_memory,
    size_t total_outputs,
    size_t chunk_size_estimate,
    double concurrency_factor = 1.5)
{
    // If free_memory is extremely large, we allow maximum concurrency
    if (free_memory == std::numeric_limits<Int64>::max())
        return total_outputs;

    // Heuristic: if free_memory < chunk_size_estimate * concurrency_factor * total_outputs,
    // we reduce concurrency proportionally.
    // For example:
    //   desired_active = (free_memory / concurrency_factor) / chunk_size_estimate
    // with a lower bound of at least 1 (unless we truly can't proceed).
    if (chunk_size_estimate == 0)
        chunk_size_estimate = 64 * 1024; // fallback if we haven't measured chunk sizes

    double ratio = static_cast<double>(free_memory) / (chunk_size_estimate * concurrency_factor);
    size_t desired = (ratio > 0) ? static_cast<size_t>(std::floor(ratio)) : 0;
    // Cap at total_outputs
    desired = std::min(desired, total_outputs);
    return desired;
}

// Helper method to count how many outputs are active
size_t MemoryDependentResizeProcessor::countActiveOutputs() const
{
    size_t active = 0;
    for (size_t i = 0; i < output_ports.size(); ++i)
    {
        if (is_output_enabled[i] && output_ports[i].status != OutputStatus::Finished)
            ++active;
    }
    return active;
}

IProcessor::Status MemoryDependentResizeProcessor::prepare(
    const PortNumbers & updated_inputs,
    const PortNumbers & updated_outputs)
{
    /// 1. Initialize data structures
    if (!initialized)
    {
        initialized = true;

        input_ports.reserve(inputs.size());
        for (auto & in : inputs)
            input_ports.push_back({.port = &in, .status = InputStatus::NotActive});

        output_ports.reserve(outputs.size());
        is_output_enabled.resize(outputs.size(), true);

        for (auto & out : outputs)
            output_ports.push_back({.port = &out, .status = OutputStatus::NotActive});
    }

    /// 2. Possibly check memory usage an disable or re-enable some outputs
    Int64 free_memory = getFreeMemory();

    size_t active_count = countActiveOutputs();

    bool memory_is_critically_low = false;

    if (free_memory <= 0)
        memory_is_critically_low = true;
    else
    {
        // Calculate threshold based on chunk size, concurrency factor, and active outputs
        if (active_count == 0)
            active_count = 1; // Ensure at least one active output is considered

        size_t local_chunk_size = (chunk_size == 0) ? 64 * 1024 : chunk_size;
        double concurrency_factor = 1.5;
        double safety_factor      = 1.5;  // Optional safety margin

        double threshold = static_cast<double>(local_chunk_size)
                           * concurrency_factor
                           * safety_factor
                           * active_count;

        if (static_cast<double>(free_memory) < threshold)
            memory_is_critically_low = true;
    }

    if (memory_is_critically_low)
    {
        // Critical memory condition: aggressively reduce concurrency
        if (active_count > 1)
        {
            size_t can_disable = active_count - 1; // Keep at least one active output

            for (size_t i = 0; i < output_ports.size() && can_disable > 0; ++i)
            {
                auto & out_info = output_ports[i];
                if (is_output_enabled[i]
                    && out_info.status != OutputStatus::Finished
                    && out_info.status != OutputStatus::Disabled)
                {
                    if (!out_info.port->hasData())
                    {
                        out_info.status = OutputStatus::Disabled;
                        is_output_enabled[i] = false;
                        --can_disable;
                    }
                }
            }
        }
    }
    else
    {
        // Not critically out of memory => possibly re-enable outputs if they were disabled previously
        size_t desired_active = calculateDesiredActiveOutputs(
            free_memory,
            output_ports.size(),
            chunk_size,
            /* concurrency_factor= */ 4);

        if (desired_active < active_count)
        {
            // We want to reduce concurrency
            size_t to_disable = active_count - desired_active;
            for (size_t i = 0; i < output_ports.size() && to_disable > 0; ++i)
            {
                auto & out_info = output_ports[i];
                if (is_output_enabled[i]
                    && out_info.status != OutputStatus::Finished
                    && out_info.status != OutputStatus::Disabled)
                {
                    /// If port buffer is empty, we can safely disable it
                    if (!out_info.port->hasData())
                    {
                        out_info.status = OutputStatus::Disabled;
                        is_output_enabled[i] = false;
                        --to_disable;
                    }
                }
            }
        }
        else if (desired_active > active_count)
        {
            // Re-enable some previously disabled outputs
            size_t to_reenable = desired_active - active_count;
            for (size_t i = 0; i < output_ports.size() && to_reenable > 0; ++i)
            {
                auto & out_info = output_ports[i];
                if (!is_output_enabled[i] && out_info.status != OutputStatus::Finished && !out_info.port->hasData())
                {
                    out_info.status = OutputStatus::NeedData;
                    is_output_enabled[i] = true;
                    waiting_outputs.push(i);
                    --to_reenable;
                }
            }
        }
    }

    /// 3. Process updated outputs
    for (auto out_idx : updated_outputs)
    {
        auto & out_info = output_ports[out_idx];

        if (out_info.port->isFinished())
        {
            if (out_info.status != OutputStatus::Finished)
            {
                out_info.status = OutputStatus::Finished;
                ++num_finished_outputs;
            }
            continue;
        }

        // If the output is still “enabled” and can accept data
        if (is_output_enabled[out_idx] && out_info.port->canPush())
        {
            if (out_info.status != OutputStatus::NeedData)
            {
                out_info.status = OutputStatus::NeedData;
                waiting_outputs.push(out_idx);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & in : inputs)
            in.close();
        return Status::Finished;
    }

    /// 4. Process updated inputs
    for (auto in_idx : updated_inputs)
    {
        auto & in_info = input_ports[in_idx];

        if (in_info.port->isFinished())
        {
            if (in_info.status != InputStatus::Finished)
            {
                in_info.status = InputStatus::Finished;
                ++num_finished_inputs;
            }
            continue;
        }

        if (in_info.port->hasData())
        {
            if (in_info.status != InputStatus::HasData)
            {
                in_info.status = InputStatus::HasData;
                inputs_with_data.push(in_idx);
            }
        }
    }

    // If all inputs are finished, finish all outputs
    if (num_finished_inputs == inputs.size())
    {
        for (auto & out_info : output_ports)
            out_info.port->finish();
        return Status::Finished;
    }

    /// 5. Match waiting outputs to inputs with data
    while (!waiting_outputs.empty() && !inputs_with_data.empty())
    {
        auto out_idx = waiting_outputs.front();
        waiting_outputs.pop();

        if (!is_output_enabled[out_idx] || output_ports[out_idx].status == OutputStatus::Finished)
            continue; // skip any disabled/finished output

        auto & out_info = output_ports[out_idx];
        if (out_info.port->hasData())
            continue;
        auto in_idx = inputs_with_data.front();
        inputs_with_data.pop();

        auto & in_info = input_ports[in_idx];
        Port::Data data = in_info.port->pullData();

        if (chunk_size == 0) /// This should be done only on the first iteration
        {
            const auto & info_chunks = data.chunk.getChunkInfos().get<ChunksToSquash>()->chunks;
            for (const auto & chunk : info_chunks)
            {
                chunk_size += chunk.bytes(); /// We have a vector of chunks to merge into one chunk as ChunkInfo
            }                                /// , so we count the cumulative size of that vector as a size of the chunk
        }

        out_info.port->pushData(std::move(data));
        out_info.status = OutputStatus::NotActive;
        in_info.status = InputStatus::NotActive;

        // If the input was finished by pulling data, mark it
        if (in_info.port->isFinished())
        {
            in_info.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
    }

    /// Still have data in queue, but no outputs can accept => we are "full"
    if (!inputs_with_data.empty())
    {
        for (auto & in_info : input_ports)
        {
            if (in_info.status != InputStatus::Finished && !in_info.port->hasData())
                in_info.port->setNotNeeded();
        }
        return Status::PortFull;
    }

    /// We have waiting outputs, but no inputs with data => "NeedData"
    /// So we should ensure we setNeeded() on at least one input that isn’t finished
    if (!waiting_outputs.empty())
    {
        for (auto & in_info : input_ports)
        {
            if (in_info.status != InputStatus::Finished && !in_info.port->hasData())
                in_info.port->setNeeded();
        }
        return Status::NeedData;
    }

    /// Otherwise, stable => no queued data or waiting outputs
    /// We might still want to read from upstream, though, if we’re not done.

    for (auto & in_info : input_ports)
    {
        if (in_info.status != InputStatus::Finished)
        {
            /// We haven't read all data from this input
            in_info.port->setNeeded();
        }
    }

    return Status::PortFull;
}

}
