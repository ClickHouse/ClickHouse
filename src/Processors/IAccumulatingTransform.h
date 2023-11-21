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
    Chunk totals;
    bool has_input = false;
    bool finished_input = false;
    bool finished_generate = false;

    virtual void consume(Chunk chunk) = 0;
    virtual Chunk generate() = 0;

    /// This method can be called once per consume call. In case if some chunks are ready.
    void setReadyChunk(Chunk chunk);
    void finishConsume() { finished_input = true; }

public:
    IAccumulatingTransform(Block input_header, Block output_header);

    Status prepare() override;
    void work() override;

    /// Adds additional port for totals.
    /// If added, totals will have been ready by the first generate() call (in totals chunk).
    InputPort * addTotalsPort();

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
};

}
