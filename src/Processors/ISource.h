#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class ISource : public IProcessor
{
protected:
    OutputPort & output;
    bool has_input = false;
    bool finished = false;
    bool got_exception = false;
    Port::Data current_chunk;

    virtual Chunk generate();
    virtual std::optional<Chunk> tryGenerate();

public:
    explicit ISource(Block header);

    Status prepare() override;
    void work() override;

    OutputPort & getPort() { return output; }
    const OutputPort & getPort() const { return output; }
};

using SourcePtr = std::shared_ptr<ISource>;

}
