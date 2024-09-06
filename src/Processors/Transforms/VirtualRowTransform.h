#pragma once

#include <Processors/IInflatingTransform.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

class VirtualRowTransform : public IProcessor
{
public:
    explicit VirtualRowTransform(const Block & header);

    String getName() const override { return "VirtualRowTransform"; }

    Status prepare() override;
    void work() override;

private:
    void consume(Chunk chunk);
    Chunk generate();

    InputPort & input;
    OutputPort & output;

    Chunk current_chunk;
    bool has_input = false;
    bool generated = false;
    bool can_generate = false;

    bool is_first = false;
    Chunk temp_chunk;
};

}
