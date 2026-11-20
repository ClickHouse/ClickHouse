#pragma once

#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>

namespace DB
{

/// ROWS frames keep only the row indexes of the bounds up to date; RANGE and GROUPS frames also keep `location`.
struct FrameBounds
{
    RowPoint start;
    RowPoint end;
    bool start_visible = false;
    bool end_visible = false;
};

class Frame
{
public:
    explicit Frame(const WindowTransformParams & params_);

    void enterPartition(RowNumber partition_start);
    void advance(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition)
    {
        advance_bounds(*this, blocks, current, partition);
    }

    [[nodiscard]] const FrameBounds & bounds() const;
    [[nodiscard]] Int64 firstRowToRead() const;

private:
    using AdvanceBounds = void (*)(Frame &, const SlidingBlocks &, const RowPoint &, const PartitionBounds &);

    template <bool start_unbounded, bool end_unbounded>
    static void advanceRows(Frame & frame, const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition);
    static void advanceRangeOrGroups(Frame & frame, const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition);

    const WindowTransformParams & params;
    /// Signed offsets of ROWS and GROUPS bounds; the end one includes the `+ 1` of the exclusive end.
    int64_t start_offset = 0;
    int64_t end_offset = 1;
    /// Chosen once from the frame shape.
    AdvanceBounds advance_bounds = nullptr;
    FrameBounds frame_bounds;
};

}
