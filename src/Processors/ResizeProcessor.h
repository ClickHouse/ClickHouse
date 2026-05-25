#pragma once

#include <queue>
#include <unordered_map>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>


namespace DB
{

class Block;

/** Has arbitrary non zero number of inputs and arbitrary non zero number of outputs.
  * All of them have the same structure.
  *
  * Pulls data from arbitrary input (whenever it is ready) and pushes it to arbitrary output (whenever it is not full).
  * Doesn't do any heavy calculations.
  * Doesn't preserve an order of data.
  *
  * Examples:
  * - union data from multiple inputs to single output - to serialize data that was processed in parallel.
  * - split data from single input to multiple outputs - to allow further parallel processing.
  */
class ResizeProcessor final : public IProcessor
{
public:
    ResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs);

    String getName() const override { return "Resize"; }
    Status prepare(const UpdatedInputPorts &, const UpdatedOutputPorts &) override;

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<OutputPort *> waiting_outputs;
    std::queue<InputPort *> inputs_with_data;
    bool initialized = false;
    bool is_reading_started = false;

    enum class OutputStatus : uint8_t
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus : uint8_t
    {
        NotActive,
        HasData,
        Finished,
    };

    std::unordered_map<const InputPort *, InputStatus> input_status;
    std::unordered_map<const OutputPort *, OutputStatus> output_status;
};

/// This is an analog of ResizeProcessor, but it tries to bind one specific input to one specific output.
/// This is an attempt to keep thread locality of data, but support rebalance when some inputs are finished earlier.
/// Usually, it's N to N mapping. Probably, we can simplify the implementation because of it.
class StrictResizeProcessor final : public IProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    StrictResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
        , current_input(inputs.begin())
        , current_output(outputs.begin())
    {
    }

    StrictResizeProcessor(InputPorts inputs_, OutputPorts outputs_)
        : IProcessor(inputs_, outputs_)
        , current_input(inputs.begin())
        , current_output(outputs.begin())
    {
    }

    String getName() const override { return "StrictResize"; }
    Status prepare(const UpdatedInputPorts &, const UpdatedOutputPorts &) override;

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<InputPort *> disabled_input_ports;
    std::queue<OutputPort *> waiting_outputs;
    bool initialized = false;

    enum class OutputStatus : uint8_t
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus : uint8_t
    {
        NotActive,
        NeedData,
        Finished,
    };

    struct InputPortState
    {
        InputStatus status;
        OutputPort * waiting_output;
    };

    struct OutputPortState
    {
        OutputStatus status;
    };

    std::unordered_map<const InputPort *, InputPortState> input_port_state;
    std::unordered_map<const OutputPort *, OutputPortState> output_port_state;

    /// This field contained chunks which were read for output which had became finished while reading was happening.
    /// They will be pushed to any next waiting output.
    std::vector<Port::Data> abandoned_chunks;
};

/** Like ResizeProcessor, but limits effective parallelism for small datasets.
  * Starts by routing data to only 1 output port. Once total_rows >= min_rows_per_output
  * (or total_bytes >= min_bytes_per_output), activates all remaining output ports at once.
  *
  * The "jump to all" semantics avoid permanent imbalance in downstream aggregator hash tables:
  * a gradual one-at-a-time ramp would push the first chunks disproportionately into early
  * outputs, and the downstream merge would then have to combine N uneven partial states.
  * For heavy aggregate states (`groupArraySorted`, `uniqExact`, ...) that hurts even at scale.
  *
  * All inputs are kept active at all times so upstream parallelism is never throttled.
  * Data from inputs is collected and routed only to active output ports.
  * Once all outputs are activated, behaves identically to ResizeProcessor with zero overhead.
  */
class GradualResizeProcessor : public IProcessor
{
public:
    GradualResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs, size_t min_rows_per_output_, size_t min_bytes_per_output_);

    String getName() const override { return "GradualResize"; }

    Status prepare(const UpdatedInputPorts &, const UpdatedOutputPorts &) override;

private:
    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    bool initialized = false;
    bool all_outputs_active = false;
    bool is_reading_started = false;

    size_t num_active_outputs = 1;
    size_t total_rows_pushed = 0;
    size_t total_bytes_pushed = 0;
    size_t min_rows_per_output;
    size_t min_bytes_per_output;

    /// Active outputs waiting for data (index < num_active_outputs).
    std::queue<UInt64> waiting_outputs;
    /// Outputs that reported canPush() but are not yet activated (index >= num_active_outputs).
    std::queue<UInt64> inactive_waiting_outputs;
    /// Inputs that have data ready.
    std::queue<UInt64> inputs_with_data;

    enum class OutputStatus : uint8_t
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus : uint8_t
    {
        NotActive,
        HasData,
        Finished,
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    std::unordered_map<const InputPort *, UInt64> input_port_index;
    std::unordered_map<const OutputPort *, UInt64> output_port_index;

    void maybeActivateMoreOutputs();
    void promoteInactiveWaitingOutputs();
};

}
