#pragma once
#include <Processors/IProcessor.h>

namespace DB
{

/// Transform which can generate several chunks on every consumed.
/// It can be assumed that class is used in following way:
///
///    for (chunk : input_chunks)
///    {
///        transform.consume(chunk);
///        while (transform.canGenerate())
///        {
///            transformed_chunk = transform.generate();
///            ... (process transformed chunk)
///        }
///    }
///    transformed_chunk = transform.getRemaining();
///    ... (process remaining data)
///
class IInflatingTransform : public IProcessor
{
protected:
    InputPort & input;
    OutputPort & output;

    Chunk current_chunk;
    bool has_input = false;
    bool generated = false;
    bool can_generate = false;

    virtual void consume(Chunk chunk) = 0;
    virtual bool canGenerate() = 0;
    virtual Chunk generate() = 0;
    virtual Chunk getRemaining() { return {}; }

public:
    IInflatingTransform(Block input_header, Block output_header);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    /// canGenerate can flush data when input is finished.
    bool is_finished = false;
};

}
