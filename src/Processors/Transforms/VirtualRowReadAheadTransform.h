#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <queue>
#include <unordered_map>

namespace DB
{

/// Sits between the in-order MergeTree sources and the merge. Each source starts by sending
/// a virtual row: a single row with the sort key of its future output, built from the primary
/// key index without reading any data. The merge keeps such a source parked until the merge
/// actually reaches that key, so a source it never reaches is never read. On its own, though,
/// the merge wakes the parked sources strictly one at a time, so a scan over many parts runs
/// sequentially. This transform owns one buffered lane per source and wakes lanes ahead of
/// the merge:
///
/// - a lane starts reading when the merge asks for it and its buffer is empty;
/// - a lane that feeds the merge keeps reading ahead, so its next read overlaps with merging;
/// - once the merge has moved to a second lane (so the limit is clearly not answered by the
///   first lane alone), up to `read_ahead_window` parked lanes nearest to the merge in key
///   order read ahead in parallel.
///
/// With `read_in_order_use_virtual_row_per_block` a source sends a virtual row after every
/// block. A "group" here means what a source produces between two virtual rows: one block
/// and the announcement after it (without the per-block mode, everything after the initial
/// virtual row is a single group).
class VirtualRowReadAheadTransform final : public IProcessor
{
public:
    VirtualRowReadAheadTransform(
        SharedHeader header_,
        size_t num_lanes,
        SortDescription description_,
        bool apply_virtual_row_conversions_,
        UInt64 limit_,
        size_t max_rows_to_buffer_,
        size_t max_bytes_to_buffer_,
        size_t read_ahead_window_);

    String getName() const override { return "VirtualRowReadAhead"; }
    Status prepare() override;
    /// Processes only the lanes whose ports changed; the overload above is the full pass
    /// over all lanes, kept for the first call and the (rare) terminal events.
    Status prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs) override;

private:
    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;

        std::queue<Chunk> buffer;
        size_t buffered_rows = 0;
        size_t buffered_bytes = 0;
        /// Rows pulled from the input; once it reaches the limit the input is closed,
        /// because a merge never needs more than `limit` rows from any single source.
        size_t num_processed_rows = 0;

        /// Sort key of the latest virtual row (1-row columns in sort description order).
        /// A lane without one either did not produce its initial virtual row yet or has none
        /// at all (plain source); such lanes are read on demand without deferral bookkeeping.
        Columns boundary;

        /// Permission to read one group ahead; consumed by the next virtual row, granted
        /// again when the merge asks for the lane, when the lane just fed the merge, and by
        /// `topUpReadAhead`. So the window limits how many lanes speculate at once, and the
        /// buffer caps limit how much real data a lane accumulates ahead of the merge. A lane
        /// that never produces virtual rows keeps its credit and streams like a plain buffer.
        size_t credit = 0;

        /// The current credit was granted by `topUpReadAhead` and the merge has not reached
        /// this lane since: only such lanes count toward `read_ahead_window`, so a window of
        /// N really means N sources reading ahead of the merge (the lane feeding the merge,
        /// and lanes that never announce boundaries, hold demand-driven credit outside it).
        bool speculative = false;
    };

    /// Returns true if any port state changed (more progress may be possible).
    bool processLane(size_t lane_num);
    void onMiss(size_t lane_num);
    bool underBufferCaps(const Lane & lane) const
    {
        return lane.buffered_rows < max_rows_to_buffer || lane.buffered_bytes < max_bytes_to_buffer;
    }
    Status tryFinish();
    void grantCredit(size_t lane_num, bool speculative = false);
    /// Returns true if the lane was not yet in the touched set of this prepare.
    bool touchLane(size_t lane_num);
    void topUpReadAhead();
    bool speculationAllowed() const { return read_ahead_window > 0; }
    bool boundaryLess(const Lane & lhs, const Lane & rhs) const;
    Columns extractBoundary(const Chunk & chunk) const;

    const SortDescription description;
    const bool apply_virtual_row_conversions;
    const UInt64 limit;
    const size_t max_rows_to_buffer;
    const size_t max_bytes_to_buffer;
    const size_t read_ahead_window;

    std::vector<Lane> lanes;

    /// Read-ahead beyond the demanded lane starts only after demand has visited two distinct
    /// lanes (or with no limit): until then the limit may be answered by the front lane alone
    /// and the other lanes must stay unread.
    ssize_t first_miss_lane = -1;
    bool cross_lane_read_ahead = false;

    /// Lanes are positionally aligned with the ports, but the ports live in std::lists and
    /// the partial `prepare` receives pointers, so this is the O(1) reverse index
    /// (`getInputPortNumber` would walk the list on every event).
    std::unordered_map<const Port *, size_t> port_to_lane;
    std::vector<UInt64> lane_touch_epoch;
    std::vector<size_t> touched_lanes;
    /// Lanes `grantCredit` woke during the running fixpoint pass; they join `touched_lanes`
    /// between passes, so no pass appends to the set it iterates.
    std::vector<size_t> credited_lanes;
    UInt64 touch_epoch = 0;
    bool initialized = false;
};

}
