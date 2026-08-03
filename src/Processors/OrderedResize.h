#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/** Splits a single stream into `num_streams` streams, dispatching whole chunks in round-robin order.
  * `OrderedGatherProcessor` takes the chunks back in the same order, so the transforms in between
  * are applied by several threads without changing the order of the rows.
  * Every transform in between must produce exactly one chunk per input chunk.
  */
class OrderedScatterProcessor final : public IProcessor
{
public:
    OrderedScatterProcessor(SharedHeader header, size_t num_streams)
        : IProcessor(InputPorts(1, header), OutputPorts(num_streams, header))
        , current_output(outputs.begin())
    {
    }

    String getName() const override { return "OrderedScatter"; }

    Status prepare() override;

private:
    OutputPorts::iterator current_output;
};

class OrderedGatherProcessor final : public IProcessor
{
public:
    OrderedGatherProcessor(SharedHeader header, size_t num_streams)
        : IProcessor(InputPorts(num_streams, header), OutputPorts(1, header))
        , current_input(inputs.begin())
    {
    }

    String getName() const override { return "OrderedGather"; }

    Status prepare() override;

private:
    InputPorts::iterator current_input;
};

}
