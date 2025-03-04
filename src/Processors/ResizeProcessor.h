#pragma once

#include <queue>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>


namespace DB
{

class BaseResizeProcessor : public IProcessor
{
public:
    BaseResizeProcessor(const Block & header_, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header_), OutputPorts(num_outputs, header_))
        , header(header_)
        , current_input(inputs.begin())
        , current_output(outputs.begin())
    {
        is_output_enabled.resize(num_outputs, true);
    }

    String getName() const override = 0;
    virtual bool isMemoryDependent() const { return false; }

protected:
    virtual void concurrencyControlLogic() {}

    Status prepareRoundRobin();
    Status prepareWithQueues(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs);

    void limitThreadsBasedOnMemory();

    Block header;

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
        Disabled, /// Used only by memory-based logic
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

    std::vector<bool> is_output_enabled;
};


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
class ResizeProcessor final : public BaseResizeProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    ResizeProcessor(const Block & header_, size_t num_inputs, size_t num_outputs)
        : BaseResizeProcessor(header_, num_inputs, num_outputs)
    {}

    String getName() const override { return "Resize"; }
    bool isMemoryDependent() const override { return false; }

    Status prepare() override { return prepareRoundRobin(); }
    Status prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs) override
    {
        return prepareWithQueues(updated_inputs, updated_outputs);
    }

protected:
    /// No concurrency logic needed
    void concurrencyControlLogic() override
    {
        // Do nothing
    }
};

/** Has arbitrary non zero number of inputs and arbitrary non zero number of outputs.
  * All of them have the same structure.
  * When the amount of free memory is too low (some heuristics implemented), we push data to lower amount of inputs, trying to slow down the process.
  */
class MemoryDependentResizeProcessor final : public BaseResizeProcessor
{
public:
    MemoryDependentResizeProcessor(const Block & header_, size_t num_inputs, size_t num_outputs)
        : BaseResizeProcessor(header_, num_inputs, num_outputs)
    {}

    ~MemoryDependentResizeProcessor() override = default;

    String getName() const override { return "MemoryDependentResize"; }
    bool isMemoryDependent() const override { return true; }

protected:
    static bool checkMemory();

    Status prepare() override { return prepareRoundRobin(); }
    Status prepare(const PortNumbers &, const PortNumbers &) override;

    size_t countActiveOutputs() const;

    void concurrencyControlLogic() override;
};

class StrictResizeProcessor : public IProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    StrictResizeProcessor(const Block & header, size_t num_inputs, size_t num_outputs)
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

}
