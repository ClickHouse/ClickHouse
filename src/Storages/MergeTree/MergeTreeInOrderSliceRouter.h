#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <deque>
#include <map>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// The processor between the sources of MergeTreeReadPoolInOrderSliced and the merge that reads in
/// order. Input i is source i; output l is lane l, one part, whose chunks come out in mark order as
/// if one source read that part alone (in reverse order, from the last mark down).
///
/// The router owns the chunks and the demand; the pool owns marks, keys and readers.
///
/// Chunks. A chunk carries its slice (MergeTreeSliceInfo) and is buffered in it. A lane's chunks
/// leave through its first unconsumed slice only, so the lane is in mark order whichever sources
/// read it. A slice is complete when its source reports it ended, in the marker it emits after
/// asking for the next slice. When a lane can take a chunk and none is ready, a virtual row
/// announces the key its next rows start at, so the merge moves on to other lanes up to that key.
///
/// Demand. Every round the pool is told what to cut (MergeTreeReadPoolInOrderSliced::Demand):
/// - the lanes the merge waits for with nothing in flight: one slice each, always;
/// - a read-ahead allowance in marks: budget minus marks in flight, where
///   budget = min(sources x full slice, 4 x consumed + 16 x empty),
///   consumed = marks of slices the merge took in full and then asked past,
///   empty = marks of slices that came back without rows.
/// The allowance is zero in a round where no lane asked anew or waits, so a LIMIT met by the last
/// chunk triggers no reads.
///
/// Sources. An input is set needed when the pool names its source to wake for a slice in the FIFO,
/// and set not needed again when the source reports idle while the FIFO is empty.
class MergeTreeInOrderSliceRouter final : public IProcessor
{
public:
    MergeTreeInOrderSliceRouter(
        SharedHeader header, std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_, ExpressionActionsPtr virtual_row_conversions_);

    String getName() const override { return "MergeTreeInOrderSliceRouter"; }
    Status prepare() override;

private:
    /// A slice from the cut until the merge consumed it.
    struct Slice
    {
        /// Marks when cut, in flight until the slice is consumed or dropped.
        size_t marks = 0;
        /// The mark the rows of the slice start at in reading order; announced while they are in flight.
        size_t boundary_mark = 0;
        std::deque<Chunk> chunks = {};
        size_t rows = 0;
        bool complete = false;
    };

    using Slices = std::map<size_t, Slice>;

    struct Lane
    {
        /// Slices cut and not yet consumed, by their number in reading order.
        Slices slices;
        /// Marks of the slices with rows the merge took in full since it last asked for the lane. They
        /// become consumed when it asks again: then it went through them and wanted more.
        size_t taken_marks = 0;
        /// The primary key announced to the merge last; empty before the first announcement.
        Block announced_key;
        /// The merge waits for this lane right now and nothing is ready for it.
        bool blocked = false;
    };

    /// The slice leaves the lane and the read-ahead, and counts as evidence.
    void dropSlice(size_t lane, Slices::iterator slice);
    void finishLane(size_t lane);
    /// Called once every lane is finished; ends the sources.
    Status finish();

    const std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool;
    /// Never empty: the pool is used only with virtual rows, see ReadFromMergeTree::slicedPoolRequested.
    const ExpressionActionsPtr virtual_row_conversions;
    /// A full slice per source.
    const size_t max_marks_in_flight;

    std::vector<InputPort *> source_inputs;
    std::vector<OutputPort *> lane_outputs;
    /// Whether each input is set needed; InputPort does not tell.
    std::vector<bool> input_needed;
    std::vector<Lane> lanes;
    /// Marks of the slices cut and not yet consumed by the merge in full.
    size_t marks_in_flight = 0;
    /// Marks of the slices the merge went through and asked past, and of the slices that came back
    /// without any rows: the evidence the read-ahead follows.
    size_t consumed_marks = 0;
    size_t empty_marks = 0;
    /// Some lane was read to its end and the merge asked for more.
    bool spans_lanes = false;
    /// The pool is finished and the idle sources were woken up to end their streams.
    bool finishing = false;
};

}
