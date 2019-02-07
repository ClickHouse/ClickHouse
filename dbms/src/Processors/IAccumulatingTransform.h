#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/** Has one input and one output.
  * Pulls all blocks from input, and only then produce output.
  * Examples: ORDER BY, GROUP BY.
  */
class IAccumulatingTransform : public IProcessor
{
protected:
    InputPort & input;
    OutputPort & output;

    Block current_input_block;
    Block current_output_block;
    bool has_input = false;
    bool finished_input = false;
    bool finished_generate = false;

    virtual void consume(Block block) = 0;
    virtual Block generate() = 0;

public:
    IAccumulatingTransform(Block input_header, Block output_header);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
};

}
