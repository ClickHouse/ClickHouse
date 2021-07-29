#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class ISink : public IProcessor
{
protected:
    InputPort & input;
    Chunk current_chunk;
    bool has_input = false;
    bool was_on_start_called = false;
    bool was_on_finish_called = false;

    virtual void consume(Chunk block) = 0;
    virtual void onStart() {}
    virtual void onFinish() {}

public:
    explicit ISink(Block header);

    Status prepare() override;
    void work() override;

    InputPort & getPort() { return input; }
};

}
