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

    Chunk current_input_chunk;
    Chunk current_output_chunk;
    bool has_input = false;
    bool finished_input = false;
    bool finished_generate = false;

    virtual void consume(Chunk chunk) = 0;
    virtual Chunk generate() = 0;

public:
    IAccumulatingTransform(Block input_header, Block output_header);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
};

}
