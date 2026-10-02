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
    explicit Frame(const WindowTransformParams & params_);

    void enterPartition(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition);
    void advance(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition);

    const FrameBounds & bounds() const;

private:
    const WindowTransformParams & params;
    FrameBounds frame_bounds;
};

}
