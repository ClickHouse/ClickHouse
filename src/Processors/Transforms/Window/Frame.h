#pragma once

#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>


namespace DB
{

/// Rows-range of the frame seen so far.
struct FrameBounds
{
    RowPoint start;
    RowPoint end;
    bool fully_visible = false;
};

class Frame
{
public:
    Frame(const WindowTransformParams & params_, const WindowFrame & definition_);

    void enterPartition(RowNumber partition_start);
    void advance(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition);

    const FrameBounds & bounds() const;

private:
    const WindowTransformParams & params;
    const WindowFrame definition;
    FrameBounds frame_bounds;
};

}
