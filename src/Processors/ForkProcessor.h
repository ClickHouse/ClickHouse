#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/** Has one input and arbitrary non zero number of outputs.
  * All of them have the same structure.
  *
  * Pulls data input and copies it to every output.
  * You may have heard about it under the name 'tee'.
  *
  * Doesn't do any heavy calculations.
  * Preserves an order of data.
  */
class ForkProcessor : public IProcessor
{
public:
    ForkProcessor(const Block & header, size_t num_outputs)
        : IProcessor(InputPorts{header}, OutputPorts(num_outputs, header))
    {
    }

    String getName() const override { return "Fork"; }

    Status prepare() override;

    InputPort & getInputPort() { return inputs.front(); }
};

}


