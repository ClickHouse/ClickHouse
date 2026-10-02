#pragma once

#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>

namespace DB
{

/// Rows-range of the current row's peer group seen so far.
struct PeersBounds
{
    RowPoint start;
    RowPoint end;
    bool fully_visible = false;
};

class Peers
{
public:
    void enterPartition(const SlidingBlocks & blocks, const PartitionBounds & partition);
    void advancePeerGroupBounds(const SlidingBlocks & blocks, const PartitionBounds & partition);
    void advanceCurrentRow(const SlidingBlocks & blocks, const PartitionBounds & partition);

    const RowPoint & current() const;
    const PeersBounds & bounds() const;

private:
    RowPoint current_row;
    PeersBounds peers_bounds;
};

}
