#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class ISink : public IProcessor
{
protected:
    InputPort & input;
    Chunk current_chunk;
    bool has_input;

    virtual void consume(Chunk block) = 0;

public:
    ISink(Block header);

    Status prepare() override;
    void work() override;

    InputPort & getPort() { return input; }
};

}
