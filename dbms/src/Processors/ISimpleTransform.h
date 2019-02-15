#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/** Has one input and one output.
  * Simply pull a block from input, transform it, and push it to output.
  */
class ISimpleTransform : public IProcessor
{
protected:
    InputPort & input;
    OutputPort & output;

    Block current_block;
    bool has_input = false;
    bool transformed = false;

    virtual void transform(Block & block) = 0;

public:
    ISimpleTransform(Block input_header, Block output_header);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
};

}
