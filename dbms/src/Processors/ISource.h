#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class ISource : public IProcessor
{
public:
    ISource(Block header);

    Status prepare() override;
    void work() override;

    OutputPort & getPort() { return output; }
    const OutputPort & getPort() const { return output; }

protected:
    OutputPort & output;

private:
    bool finished = false;
    bool has_input = false;
    Port::Data current_chunk;

    /// If chunk is not set, then we're finished.
    /// We allow to return empty chunks this way, which is required for streaming sources.
    virtual std::optional<Chunk> generate() = 0;
};

using SourcePtr = std::shared_ptr<ISource>;

}
