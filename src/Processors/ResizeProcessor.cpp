#include <Processors/ResizeProcessor.h>
#include <algorithm>

#include <Processors/Port.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// TODO Check that there is non zero number of inputs and outputs.
ResizeProcessor::ResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    , current_input(inputs.begin())
    , current_output(outputs.begin())
{
}

IProcessor::Status ResizeProcessor::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_status[&input] = InputStatus::NotActive;

        for (auto & output : outputs)
            output_status[&output] = OutputStatus::NotActive;
    }

    for (auto * output_port : updated_outputs)
    {
        OutputStatus & status = output_status.at(output_port);
        if (output_port->isFinished())
        {
            if (status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                status = OutputStatus::Finished;
            }

            continue;
        }

        if (output_port->canPush())
        {
            if (status != OutputStatus::NeedData)
            {
                status = OutputStatus::NeedData;
                waiting_outputs.push(output_port);
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

    for (auto * input_port : updated_inputs)
    {
        auto & status = input_status.at(input_port);
        if (input_port->isFinished())
        {
            if (status != InputStatus::Finished)
            {
                status = InputStatus::Finished;
                ++num_finished_inputs;
            }
            continue;
        }

        if (input_port->hasData())
        {
            if (status != InputStatus::HasData)
            {
                status = InputStatus::HasData;
                inputs_with_data.push(input_port);
            }
        }
    }

    while (!waiting_outputs.empty() && !inputs_with_data.empty())
    {
        auto * waiting_output = waiting_outputs.front();
        waiting_outputs.pop();

        auto * input_with_data = inputs_with_data.front();
        inputs_with_data.pop();

        waiting_output->pushData(input_with_data->pullData());
        input_status.at(input_with_data) = InputStatus::NotActive;
        output_status.at(waiting_output) = OutputStatus::NotActive;

        if (input_with_data->isFinished())
        {
            input_status.at(input_with_data) = InputStatus::Finished;
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

IProcessor::Status StrictResizeProcessor::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_port_state[&input] = {.status = InputStatus::NotActive, .waiting_output = nullptr};

        for (auto & input : inputs)
            disabled_input_ports.push(&input);

        for (auto & output : outputs)
            output_port_state[&output] = {.status = OutputStatus::NotActive};
    }

    for (auto * output_port : updated_outputs)
    {
        auto & state = output_port_state.at(output_port);
        if (output_port->isFinished())
        {
            if (state.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                state.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output_port->canPush())
        {
            if (state.status != OutputStatus::NeedData)
            {
                state.status = OutputStatus::NeedData;
                state.is_waiting = true;
                waiting_outputs.push(output_port);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    std::queue<InputPort *> inputs_with_data;

    for (auto * input_port : updated_inputs)
    {
        auto & state = input_port_state.at(input_port);
        if (input_port->isFinished())
        {
            if (state.status != InputStatus::Finished)
            {
                state.status = InputStatus::Finished;
                ++num_finished_inputs;

                /// Release the output this input was paired with, so that it can be handed to
                /// another input or closed. Only an output that is free right now may be released:
                /// an output in the `NotActive` state still holds a chunk that the downstream
                /// processor has not consumed yet, and handing it to another input would break the
                /// invariant checked below. Such an output is enqueued by the loop over
                /// `updated_outputs` above as soon as it becomes pushable again. An input that was
                /// never paired with an output has no output to release.
                if (state.waiting_output)
                {
                    auto & released_state = output_port_state.at(state.waiting_output);
                    if (released_state.status == OutputStatus::NeedData && !released_state.is_waiting)
                    {
                        released_state.is_waiting = true;
                        waiting_outputs.push(state.waiting_output);
                    }
                }
            }
            continue;
        }

        if (input_port->hasData())
        {
            if (state.status != InputStatus::NotActive)
            {
                state.status = InputStatus::NotActive;
                inputs_with_data.push(input_port);
            }
        }
    }

    while (!inputs_with_data.empty())
    {
        auto * input_port = inputs_with_data.front();
        auto & input_state = input_port_state.at(input_port);
        inputs_with_data.pop();

        if (input_state.waiting_output == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No associated output for input with data");

        auto * waiting_output = input_state.waiting_output;
        auto & output_state = output_port_state.at(waiting_output);

        /// The pairing is consumed by this chunk. If the pointer were kept, a later `isFinished`
        /// on this input would release an output that has meanwhile been handed to another input.
        input_state.waiting_output = nullptr;

        if (output_state.status == OutputStatus::NotActive)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status NotActive for associated output");

        if (output_state.status != OutputStatus::Finished)
        {
            waiting_output->pushData(input_port->pullData(/* set_not_needed = */ true));
            output_state.status = OutputStatus::NotActive;
        }
        else
            abandoned_chunks.emplace_back(input_port->pullData(/* set_not_needed = */ true));

        if (input_port->isFinished())
        {
            input_state.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
        else
            disabled_input_ports.push(input_port);
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// `waiting_outputs` is a lazy queue: an output may have finished after it was enqueued (the
    /// loop over `updated_outputs` above flips its status but cannot remove it from the middle of
    /// the queue), so every consumer below re-checks the current status and skips stale entries.

    /// Process abandoned chunks if any.
    while (!abandoned_chunks.empty() && !waiting_outputs.empty())
    {
        auto * waiting_output = waiting_outputs.front();
        auto & output_state = output_port_state.at(waiting_output);
        waiting_outputs.pop();
        output_state.is_waiting = false;

        if (output_state.status != OutputStatus::NeedData)
            continue;

        waiting_output->pushData(std::move(abandoned_chunks.back()));
        abandoned_chunks.pop_back();

        output_state.status = OutputStatus::NotActive;
    }

    /// Enable more inputs if needed.
    /// `disabled_input_ports` is a lazy queue as well: an input may finish while it is sitting in
    /// the queue, because an upstream output is allowed to `finish` even when the peer input is not
    /// needed. The loop over `updated_inputs` above flips such an input to `Finished` but cannot
    /// remove it from the middle of the queue, so stale entries are skipped here. Pairing an output
    /// with an already finished input would strand the output forever and could deadlock the
    /// processor, and it would also let the input be counted as finished twice.
    while (!disabled_input_ports.empty() && !waiting_outputs.empty())
    {
        auto * input_port = disabled_input_ports.front();
        auto & input_state = input_port_state.at(input_port);

        if (input_state.status == InputStatus::Finished)
        {
            disabled_input_ports.pop();
            continue;
        }

        auto * waiting_output = waiting_outputs.front();
        auto & output_state = output_port_state.at(waiting_output);
        waiting_outputs.pop();
        output_state.is_waiting = false;

        if (output_state.status != OutputStatus::NeedData)
            continue;

        disabled_input_ports.pop();

        input_port->setNeeded();
        input_state.status = InputStatus::NeedData;
        input_state.waiting_output = waiting_output;
    }

    /// Close all other waiting for data outputs (there is no corresponding input for them).
    while (!waiting_outputs.empty())
    {
        auto * output_port = waiting_outputs.front();
        auto & output_state = output_port_state.at(output_port);
        waiting_outputs.pop();
        output_state.is_waiting = false;

        if (output_state.status != OutputStatus::Finished)
           ++num_finished_outputs;

        output_state.status = OutputStatus::Finished;
        output_port->finish();
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

GradualResizeProcessor::GradualResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs, size_t min_rows_per_output_, size_t min_bytes_per_output_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    , all_outputs_active(num_outputs <= 1)
    , strict_routing_possible(num_inputs == num_outputs)
    , min_rows_per_output(min_rows_per_output_)
    , min_bytes_per_output(min_bytes_per_output_)
{
}

void GradualResizeProcessor::maybeActivateMoreOutputs()
{
    if (num_active_outputs >= output_ports.size())
        return;

    /// Once the per-output threshold is crossed, activate all outputs at once instead of
    /// ramping up one at a time. Gradual ramp-up causes permanent imbalance in downstream
    /// aggregator hash tables: chunks pushed during the ramp land disproportionately in
    /// early outputs, and the downstream merge then combines N uneven partial states. For
    /// heavy aggregate states (`groupArraySorted`, `uniqExact`, ...) the merge cost scales
    /// super-linearly with table size, so the early skew hurts even when total data is large.
    bool rows_threshold = min_rows_per_output > 0 && total_rows_pushed >= min_rows_per_output;
    bool bytes_threshold = min_bytes_per_output > 0 && total_bytes_pushed >= min_bytes_per_output;

    if (!rows_threshold && !bytes_threshold)
        return;

    num_active_outputs = output_ports.size();
    all_outputs_active = true;
}

void GradualResizeProcessor::promoteInactiveWaitingOutputs()
{
    std::queue<UInt64> remaining;
    while (!inactive_waiting_outputs.empty())
    {
        auto idx = inactive_waiting_outputs.front();
        inactive_waiting_outputs.pop();

        if (output_ports[idx].status == OutputStatus::Finished)
            continue;

        if (all_outputs_active || idx < num_active_outputs)
            waiting_outputs.push(idx);
        else
            remaining.push(idx);
    }
    inactive_waiting_outputs = std::move(remaining);
}

bool GradualResizeProcessor::anyOutputNeedsData() const
{
    return std::any_of(
        output_ports.begin(), output_ports.end(),
        [](const OutputPortWithStatus & output) { return output.status == OutputStatus::NeedData; });
}

void GradualResizeProcessor::transferData(InputPortWithStatus & input_with_data, OutputPortWithStatus & output)
{
    auto data = input_with_data.port->pullData();

    if (!all_outputs_active)
    {
        if (min_rows_per_output > 0)
            total_rows_pushed += data.chunk.getNumRows();
        /// `Chunk::bytes` iterates all columns, so skip it when the bytes threshold is disabled.
        if (min_bytes_per_output > 0)
            total_bytes_pushed += data.chunk.bytes();
    }

    output.port->pushData(std::move(data));
    input_with_data.status = InputStatus::NotActive;
    output.status = OutputStatus::NotActive;

    if (input_with_data.port->isFinished())
    {
        input_with_data.status = InputStatus::Finished;
        ++num_finished_inputs;
    }
}

void GradualResizeProcessor::routeData()
{
    if (strictRouting())
        routeDataStrict();
    else
        routeDataManyToMany();
}

void GradualResizeProcessor::routeDataManyToMany()
{
    while (!waiting_outputs.empty() && !inputs_with_data.empty())
    {
        auto & waiting_output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        /// An output may have been served or have finished after it was queued, so re-check it.
        if (waiting_output.status != OutputStatus::NeedData)
            continue;

        auto & input_with_data = input_ports[inputs_with_data.front()];
        inputs_with_data.pop();

        transferData(input_with_data, waiting_output);
    }
}

/// Steady-state routing: input `i` is paired with output `i`, exactly like the strict resize that
/// the pre-aggregation stage uses when gradual resize is disabled. Chunks of one input are never
/// spread over several `AggregatingTransform`s, so data that is localized by the grouping key keeps
/// every key inside a single partial hash table and the final merge has nothing to combine per key.
/// Many-to-many routing instead scatters every key over all streams, which makes the merge of heavy
/// aggregate states (`groupArraySorted`, `uniqExact`, ...) super-linearly more expensive - it was
/// measured as up to a 2.2x slowdown of `groupArraySorted` over sorted data at `max_threads` 8 and 16.
void GradualResizeProcessor::routeDataStrict()
{
    /// These queues carry no information that `output_ports[i].status` does not already carry, and
    /// they are not consulted below. Dropping their entries here also keeps them from growing
    /// without bound over a long query, because nothing would consume them any more.
    waiting_outputs = {};
    inactive_waiting_outputs = {};

    std::queue<UInt64> deferred;
    while (!inputs_with_data.empty())
    {
        const auto input_number = inputs_with_data.front();
        inputs_with_data.pop();

        auto & input_with_data = input_ports[input_number];
        auto & paired_output = output_ports[input_number];

        if (paired_output.status == OutputStatus::NeedData)
        {
            transferData(input_with_data, paired_output);
            continue;
        }

        if (paired_output.status == OutputStatus::Finished)
        {
            /// The paired output is gone, so locality cannot be preserved for this chunk any more.
            /// Hand it to any other output that needs data instead of stranding it - the data still
            /// has to be aggregated somewhere.
            auto fallback = std::find_if(
                output_ports.begin(), output_ports.end(),
                [](const OutputPortWithStatus & output) { return output.status == OutputStatus::NeedData; });

            if (fallback != output_ports.end())
            {
                transferData(input_with_data, *fallback);
                continue;
            }
        }

        if (paired_output.status == OutputStatus::NotActive)
        {
            /// The paired output is busy. If the input paired with some other output has finished,
            /// that output would otherwise stay idle for the rest of the query, and the stage would
            /// shrink to the number of still-running inputs - e.g. for parts of different sizes or
            /// for an upstream filter that empties some streams early. Hand the chunk to such an
            /// orphaned output, like `StrictResizeProcessor` does when a finished input releases its
            /// output. Outputs whose input is still running are left to that input.
            auto orphaned = std::find_if(
                output_ports.begin(), output_ports.end(),
                [&](const OutputPortWithStatus & output)
                {
                    return output.status == OutputStatus::NeedData
                        && input_ports[&output - output_ports.data()].status == InputStatus::Finished;
                });

            if (orphaned != output_ports.end())
            {
                transferData(input_with_data, *orphaned);
                continue;
            }
        }

        /// Keep the chunk in the input port and wait for the paired output.
        deferred.push(input_number);
    }

    inputs_with_data = std::move(deferred);
}

/// During the ramp-up this implementation uses ResizeProcessor-like many-to-many routing.
/// All inputs are kept active at all times so upstream parallelism is never throttled.
/// Data is collected from any input and routed only to active (gradually activated) outputs.
/// Once all outputs have been activated, routing becomes one-to-one - see `routeDataStrict`.
IProcessor::Status GradualResizeProcessor::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
        {
            input_port_index[&input] = input_ports.size();
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive});
        }

        for (auto & output : outputs)
        {
            output_port_index[&output] = output_ports.size();
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
        }
    }

    /// 1. Process updated outputs.
    for (const auto * output_port : updated_outputs)
    {
        const auto output_number = output_port_index.at(output_port);
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;

                /// If an active output finishes, we need to activate another one to avoid deadlock.
                /// Otherwise, if all active outputs finish before thresholds grow, no data can flow.
                if (!all_outputs_active && output_number < num_active_outputs)
                {
                    while (num_active_outputs < output_ports.size())
                    {
                        size_t candidate = num_active_outputs;
                        ++num_active_outputs;
                        if (output_ports[candidate].status != OutputStatus::Finished)
                            break;
                    }
                    if (num_active_outputs >= output_ports.size())
                        all_outputs_active = true;

                    /// Newly activated outputs may already have requested data and be sitting in
                    /// `inactive_waiting_outputs`. Promote them now so data can flow to them
                    /// without waiting for a new updated_outputs event.
                    promoteInactiveWaitingOutputs();
                }
            }
            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;

                if (all_outputs_active || output_number < num_active_outputs)
                    waiting_outputs.push(output_number);
                else
                    inactive_waiting_outputs.push(output_number);
            }
        }
    }

    /// Start reading from all inputs once any output needs data.
    if (!is_reading_started && (strictRouting() ? anyOutputNeedsData() : !waiting_outputs.empty()))
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

    /// 2. Process updated inputs — collect data from any input that has it.
    for (const auto * input_port : updated_inputs)
    {
        const auto input_number = input_port_index.at(input_port);
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

    /// 3. Route data from inputs to outputs.
    routeData();

    /// 4. Maybe activate more outputs after pushing data.
    if (!all_outputs_active)
    {
        size_t prev_active = num_active_outputs;
        maybeActivateMoreOutputs();

        if (num_active_outputs > prev_active)
        {
            promoteInactiveWaitingOutputs();

            /// Try to push more data to the newly activated outputs.
            routeData();
        }
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    if (strictRouting() ? anyOutputNeedsData() : !waiting_outputs.empty())
        return Status::NeedData;

    return Status::PortFull;
}

}
