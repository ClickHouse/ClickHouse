#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/** Has arbitrary N inputs and M outputs.
  * All of them have empty structure.
  * Finishes outputs when all inputs are closed.
  */
class BarrierProcessor : public IProcessor
{
public:
    BarrierProcessor();

    String getName() const override { return "Barrier"; }

    Status prepare() override;

    /// Must be called before the first call to @prepare.
    InputPort & addInputPort();
    /// Must be called before the first call to @prepare.
    OutputPort & addOutputPort();

private:
    bool prepared = false;
    InputPorts::iterator next_non_finished;
};

}
