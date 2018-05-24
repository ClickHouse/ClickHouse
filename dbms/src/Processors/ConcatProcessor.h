#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/** Has arbitary non zero number of inputs and one output.
  * All of them have the same structure.
  *
  * Pulls all data from first input, then all data from second input, etc...
  * Doesn't do any heavy calculations.
  * Preserves an order of data.
  */
class ConcatProcessor : public IProcessor
{
public:
    ConcatProcessor(InputPorts inputs_, OutputPort output_)
        : IProcessor(std::move(inputs_), {std::move(output_)}), current_input(inputs.begin())
    {
    }

    String getName() const override { return "Concat"; }

    Status prepare() override;

private:
    InputPorts::iterator current_input;
};

}

