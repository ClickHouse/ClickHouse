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

    Port::Data current_data;
    bool has_input = false;
    bool transformed = false;
    bool no_more_data_needed = false;
    const bool skip_empty_chunks;

    /// Set input port NotNeeded after chunk was pulled.
    /// Input port will become needed again only after data was transformed.
    /// This allows to escape caching chunks in input port, which can lead to uneven data distribution.
    bool set_input_not_needed_after_read = false;

    virtual void transform(Chunk & chunk) = 0;
    virtual bool needInputData() const { return true; }
    void stopReading() { no_more_data_needed = true; }

public:
    ISimpleTransform(Block input_header_, Block output_header_, bool skip_empty_chunks_);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    void setInputNotNeededAfterRead(bool value) { set_input_not_needed_after_read = value; }
};

}
