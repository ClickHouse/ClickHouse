#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <deque>
#include <map>
#include <optional>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Connects the sources reading from MergeTreeReadPoolInOrderSliced to the merge that reads in order.
/// Input i is source i; output l is lane l, one part, whose chunks come out in mark order as if a
/// single source read that part alone.
///
/// The router owns the scheduling. It watches which lane outputs the merge demands, reassembles the
/// slices of a lane by their first mark, and assigns the next slice to idle sources through the pool.
/// A source runs only while its input port is needed, and the port is set needed only after a slice
/// was assigned to it, so idle sources cost nothing and no part is read before the merge wants it.
///
/// Reading of a lane grows on evidence: the first demand grants one slice of one segment. When a slice
/// ends with most of its rows filtered out, reading is the bottleneck rather than merging, so the number
/// of segments the lane may read at once doubles, and untouched lanes get one speculative slice each in
/// the order of their boundaries. A lane holding enough buffered rows is not read further.
class MergeTreeInOrderSliceRouter final : public IProcessor
{
public:
    MergeTreeInOrderSliceRouter(
        SharedHeader header,
        std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_,
        ExpressionActionsPtr virtual_row_conversions_,
        size_t limit_,
        size_t max_block_size_rows_);

    String getName() const override { return "MergeTreeInOrderSliceRouter"; }
    Status prepare() override;

private:
    struct SliceBuffer
    {
        std::deque<Chunk> chunks;
        size_t rows = 0;
        bool finished = false;
    };

    struct Lane
    {
        /// Slices in flight or buffered, by their first mark.
        std::map<size_t, SliceBuffer> slices;
        /// Announces the first key of the lane to the merge before anything is read.
        std::optional<Chunk> initial_virtual_row;
        size_t buffered_rows = 0;
        size_t delivered_rows = 0;
        /// How many segments of the lane may be read at once.
        size_t max_segments = 1;
        /// The merge asked for this lane at least once.
        bool activated = false;
        /// A slice of the lane was assigned at least once.
        bool touched = false;
        bool finished = false;
    };

    struct Assignment
    {
        size_t lane;
        size_t first_mark;
        size_t rows_in_marks;
        size_t rows_read = 0;
    };

    void initialize();
    void consumeInput(size_t source);
    void pushToLane(size_t lane);
    SliceBuffer * headSliceWithData(size_t lane);
    /// Rows buffered in slices that can be delivered without reading any earlier mark first.
    size_t deliverableRows(size_t lane) const;
    bool headWantsMore(size_t lane) const;
    bool laneWantsMore(size_t lane) const;
    size_t openSegmentsOf(size_t lane) const;
    size_t speculativeSlicesInFlight() const;
    bool coverageAllows(size_t lane) const;
    std::optional<size_t> pickIdleSource(bool allow_rebinding) const;
    void assignSlice(size_t source, size_t lane);
    void scheduleSlices();

    const std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool;
    const ExpressionActionsPtr virtual_row_conversions;
    const size_t limit;
    const size_t buffer_budget_rows;

    std::vector<InputPort *> source_inputs;
    std::vector<OutputPort *> lane_outputs;
    std::vector<Lane> lanes;
    std::vector<std::optional<Assignment>> assignments;
    /// Position of each lane in the boundary order: the lower, the sooner the merge needs it.
    std::vector<size_t> boundary_position;
    size_t num_finished_lanes = 0;
    bool initialized = false;
    /// Set once a slice ended with most rows filtered out; from then on untouched lanes are read ahead.
    bool speculation_open = false;
};

}
