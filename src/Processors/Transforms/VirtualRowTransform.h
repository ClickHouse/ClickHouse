#pragma once

#include <Processors/IProcessor.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// Virtual row is useful for read-in-order optimization when multiple parts exist.
class VirtualRowTransform : public IProcessor
{
public:
    explicit VirtualRowTransform(const Block & header_, const Block & pk_block_);

    String getName() const override { return "VirtualRowTransform"; }

    Status prepare() override;
    void work() override;

private:
    InputPort & input;
    OutputPort & output;

    Chunk current_chunk;
    bool has_input = false;
    bool generated = false;
    bool can_generate = true;
    bool is_first = true;

    Block header;
    Block pk_block;
};

}
