#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// Virtual row is useful for read-in-order optimization when multiple parts exist.
class VirtualRowTransform : public IProcessor
{
public:
    explicit VirtualRowTransform(SharedHeader header_, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_);

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

    Block pk_block;
    ExpressionActionsPtr virtual_row_conversions;
};

}
