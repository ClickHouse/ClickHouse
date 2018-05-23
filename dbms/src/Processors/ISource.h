#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class ISource : public IProcessor
{
protected:
    OutputPort & output;
    bool finished = false;
    Block current_block;

    virtual Block generate() = 0;

public:
    ISource(Block header);

    Status prepare() override;
    void work() override;

    OutputPort & getPort() { return output; }
};

}
