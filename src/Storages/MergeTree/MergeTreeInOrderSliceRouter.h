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
/// The router decides what is read and when. A source runs only while its input port is needed, and
/// the port is set needed only after a slice was assigned to it, so idle sources cost nothing and no
/// part is read before the router asks for it. Slices are issued only while the merge waits for data,
/// by two rules:
/// - the lane the merge waits for gets its next slice whenever it has none issued, so the merge is never
///   blocked on a lane nobody reads;
/// - the lanes the merge needs next, in the order of the pool's queue, get slices while the marks issued
///   so far stay under the read-ahead budget. The budget is zero at first and doubles with every slice
///   that came back with most of its rows filtered out: reading, not merging, is the bottleneck then, and
///   the longer the granules keep coming back empty, the further ahead the reading runs.
/// A slice counts as issued until the merge has taken its last row, so the budget bounds the rows held in
/// the router as well as the sources reading on behalf of the merge.
class MergeTreeInOrderSliceRouter final : public IProcessor
{
public:
    MergeTreeInOrderSliceRouter(
        SharedHeader header,
        std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_,
        ExpressionActionsPtr virtual_row_conversions_);

    String getName() const override { return "MergeTreeInOrderSliceRouter"; }
    Status prepare() override;

private:
    struct SliceBuffer
    {
        std::deque<Chunk> chunks;
        size_t marks = 0;
        bool finished = false;
    };

    using SliceBuffers = std::map<size_t, SliceBuffer>;

    struct Lane
    {
        /// Issued slices by their first mark: in flight, or finished with rows the merge has not taken yet.
        SliceBuffers slices;
        /// Announces the first key of the lane to the merge before anything is read.
        std::optional<Chunk> initial_virtual_row;
        /// The merge waits for this lane right now and nothing is ready for it.
        bool wants_data = false;
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
    void finishLane(size_t lane);
    void dropSlice(size_t lane, SliceBuffers::iterator slice);
    size_t readAheadMarks() const;
    std::optional<size_t> pickIdleSource(size_t lane) const;
    void assignSlice(size_t source, size_t lane);
    void scheduleSlices();
    /// Called once every lane is finished; ends the sources.
    Status finish();

    const std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool;
    const ExpressionActionsPtr virtual_row_conversions;

    std::vector<InputPort *> source_inputs;
    std::vector<OutputPort *> lane_outputs;
    std::vector<Lane> lanes;
    std::vector<std::optional<Assignment>> assignments;
    size_t num_finished_lanes = 0;
    /// Marks of the slices in the lanes' buffers: assigned and not yet taken by the merge in full.
    size_t issued_marks = 0;
    /// Slices that ended with most of their rows filtered out.
    size_t misses = 0;
    bool initialized = false;
};

}
