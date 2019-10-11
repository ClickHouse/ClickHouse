#pragma once

#include <Processors/IProcessor.h>


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

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;
};

}
