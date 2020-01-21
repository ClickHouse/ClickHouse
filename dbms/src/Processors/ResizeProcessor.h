#pragma once

#include <Processors/IProcessor.h>
#include <queue>


namespace DB
{

/** Has arbitary non zero number of inputs and arbitary non zero number of outputs.
  * All of them have the same structure.
  *
  * Pulls data from arbitary input (whenever it is ready) and pushes it to arbitary output (whenever is is not full).
  * Doesn't do any heavy calculations.
  * Doesn't preserve an order of data.
  *
  * Examples:
  * - union data from multiple inputs to single output - to serialize data that was processed in parallel.
  * - split data from single input to multiple outputs - to allow further parallel processing.
  */
class ResizeProcessor : public IProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    ResizeProcessor(const Block & header, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
        , current_input(inputs.begin())
        , current_output(outputs.begin())
    {
    }

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

    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus
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

class StrictResizeProcessor : public IProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    StrictResizeProcessor(const Block & header, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    {
    }

    String getName() const override { return "StrictResize"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:

    class InputPortsQueue
    {
    public:
        void init(size_t num_inputs, size_t num_outputs)
        {
            if (num_inputs == num_outputs)
            {
                queue_mode = false;
                input_flags.assign(num_inputs, true);
            }
            else
            {
                for (size_t i = 0; i < num_inputs; ++i)
                    inputs_queue.push(i);
            }
        }

        size_t pop(size_t preferred)
        {
            if (!queue_mode)
            {
                if (input_flags[preferred])
                {
                    input_flags[preferred] = false;
                    return preferred;
                }

                queue_mode = false;

                for (size_t i = 0; i < input_flags.size(); ++i)
                    if (input_flags[i])
                        inputs_queue.push(i);
            }

            preferred = inputs_queue.front();
            return preferred;
        }

        void push(size_t input)
        {
            if (queue_mode)
                inputs_queue.push(input);
            else
                input_flags[input] = true;
        }

        bool empty() const { return queue_mode && inputs_queue.empty(); }

    private:
        std::vector<char> input_flags;
        std::queue<size_t> inputs_queue;
        bool queue_mode = true;
    };

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    InputPortsQueue disabled_input_ports;
    std::queue<UInt64> waiting_outputs;
    bool initialized = false;

    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus
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
        ssize_t last_input;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
};

}
