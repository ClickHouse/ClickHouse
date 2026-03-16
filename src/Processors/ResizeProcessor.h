#pragma once

#include <queue>
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

    Status prepare() override;
    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<UInt64> waiting_outputs;
    std::queue<UInt64> inputs_with_data;
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
};

/// This is an analog of ResizeProcessor, but it tries to bind one specific input to one specific output.
/// This is an attempt to keep thread locality of data, but support rebalance when some inputs are finished earlier.
/// Usually, it's N to N mapping. Probably, we can simplify the implementation because of it.
class StrictResizeProcessor : public IProcessor
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

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<UInt64> disabled_input_ports;
    std::queue<UInt64> waiting_outputs;
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

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
        ssize_t waiting_output;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    /// This field contained chunks which were read for output which had became finished while reading was happening.
    /// They will be pushed to any next waiting output.
    std::vector<Port::Data> abandoned_chunks;
};

/** Like StrictResizeProcessor, but gradually activates output ports as data volume grows.
  * Uses strict 1:1 input-output binding (like StrictResizeProcessor) to preserve thread locality,
  * but starts by allowing only 1 output port and activates more as total_rows or total_bytes
  * exceed thresholds (min_rows_per_output * num_active_outputs, min_bytes_per_output * num_active_outputs).
  * For small datasets, only a few aggregating threads receive data; for large datasets, all are used.
  */
class GradualResizeProcessor : public IProcessor
{
public:
    GradualResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs, size_t min_rows_per_output_, size_t min_bytes_per_output_);

    String getName() const override { return "GradualResize"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    bool initialized = false;

    size_t num_active_outputs = 1;
    size_t total_rows_pushed = 0;
    size_t total_bytes_pushed = 0;
    size_t min_rows_per_output;
    size_t min_bytes_per_output;

    std::queue<UInt64> disabled_input_ports;
    std::queue<UInt64> active_waiting_outputs;
    std::queue<UInt64> inactive_waiting_outputs;

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

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
        ssize_t waiting_output;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    /// Chunks read for an output that became finished while reading was in progress.
    std::vector<Port::Data> abandoned_chunks;

    void maybeActivateMoreOutputs();
};

}
