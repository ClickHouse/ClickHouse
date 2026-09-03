#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>
#include <set>
#include <unordered_map>

namespace DB
{

/// N-in / N-out processor between the in-order `MergeTree` streams and the final
/// `MergingSortedTransform` of a read-in-order query. Lane i passes input i to output i
/// unchanged; the transform decides which lanes may read ahead of the merge, how far, and
/// buffers what they read. The merge alone consumes deferred streams one at a time, which
/// serialises a scan over many parts; this transform restores the parallelism while keeping
/// the reads wasted under a `LIMIT` bounded.
///
/// A lane's bound is the sort key of the latest chunk it pulled: the key of a virtual row, or
/// the last row of a real chunk. The K lanes with the smallest bounds (the set) read ahead up
/// to the frontier, the smallest bound of a lane outside the set: the merge cannot consume
/// beyond that key without another lane, so anything further is premature. A lane the merge
/// demands always reads. With a limit, the set starts reading only once the merge has demanded
/// a second lane parked behind a virtual row, and stops once the rows pulled reach the limit.
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
    Status prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs) override;

private:
    struct BoundLess
    {
        const VirtualRowReadAheadTransform * self;
        bool operator()(size_t lhs, size_t rhs) const;
    };

    using RankedLanes = std::set<size_t, BoundLess>;

    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;

        /// Real chunks with rows in arrival order. A pending virtual row, if any, is the last element.
        std::deque<Chunk> buffer;
        bool pending_virtual_row = false;
        size_t buffered_rows = 0;
        size_t buffered_bytes = 0;

        /// Sort key of the latest chunk pulled; empty until the first one.
        Columns bound;
        bool bound_from_virtual_row = false;
        bool ranked = false;
        RankedLanes::iterator rank_it;

        UInt64 rows_pulled = 0;
        /// Rows pulled since the latest virtual row. A virtual row arriving while this is 0 after
        /// an earlier one means the lane just passed a fully filtered block.
        UInt64 rows_since_announcement = 0;
        bool announced = false;

        bool in_set = false;
        /// Granted by a filtered stretch on the demanded lane: may pull one block, then parks.
        bool warmup_credit = false;
        bool output_finished = false;
        bool input_finished_noted = false;
        bool queued = false;
    };

    Status prepareImpl(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs);

    int compareKeys(const Columns & lhs, const Columns & rhs) const;
    Columns virtualRowKey(const Chunk & chunk) const;
    Columns lastRowKey(const Chunk & chunk) const;

    bool underCaps(const Lane & lane) const;
    bool isDemanded(const Lane & lane) const;
    bool mayRead(size_t lane_num) const;
    ssize_t frontierFor(size_t lane_num) const;

    void enqueue(size_t lane_num);
    void runLanes();
    void recomputeSet();
    void driveLane(size_t lane_num);
    void noteInputFinished(size_t lane_num);
    void pushFromBuffer(Lane & lane);
    void consume(size_t lane_num, Chunk chunk);
    void setBound(size_t lane_num, Columns key, bool from_virtual_row);
    void noteDemand(size_t lane_num);
    void grantWarmup(size_t lane_num);
    void finishLane(size_t lane_num);

    SharedHeader header;
    SortDescription description;
    std::vector<size_t> sort_column_positions;
    const bool apply_virtual_row_conversions;
    const UInt64 limit;
    const size_t max_rows_to_buffer;
    const size_t max_bytes_to_buffer;
    const size_t read_ahead_window;

    std::vector<Lane> lanes;
    std::unordered_map<const Port *, size_t> lane_by_port;

    /// Lanes with a known bound and an unfinished output, in (bound, lane index) order.
    RankedLanes ranked_lanes;
    /// The set and the two smallest-bound ranked lanes outside it (-1 = none). Two are kept so
    /// that a lane reading outside the set can take the frontier excluding itself.
    std::vector<size_t> set_lanes;
    std::vector<size_t> previous_set_lanes;
    ssize_t frontier_lanes[2] = {-1, -1};

    bool window_open;
    ssize_t first_demanded_lane = -1;
    ssize_t last_demanded_lane = -1;
    /// Rows pulled over lanes whose output is not finished.
    UInt64 budget_rows = 0;
    size_t finished_outputs = 0;
    bool initialized = false;
    /// Raised by every state transition that can change the set, the frontier or a lane's reading rights.
    bool recompute_needed = true;

    std::vector<size_t> candidates;
};

}
