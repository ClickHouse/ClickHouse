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
/// Terms:
/// - Bound of a lane: the sort key of the latest chunk it pulled, i.e. the key of its virtual
///   row or the last row of its latest block. Everything the lane produces next is >= its bound.
/// - Set: the K unfinished lanes under their caps with the smallest bounds, K being the
///   read-ahead window. They are the lanes allowed to read ahead of the merge.
/// - Frontier: the smallest bound among the lanes outside the set. The merge emits rows in key
///   order, so it cannot get past the frontier without reading the lane that owns it; a set
///   lane therefore reads only while its own bound is below the frontier, anything beyond would
///   sit in memory until that other lane is read.
/// With K = 2 and parts holding keys [0, 10), [10, 20), [20, 30), [30, 40): the set is parts 1
/// and 2, the frontier is 20, both read to their ends; when part 1 is done, part 3 enters the
/// set and the frontier moves to 30.
///
/// A lane the merge demands always reads. With a limit, the set starts reading only once the
/// merge has demanded a second lane parked behind a virtual row, and stops once the rows
/// pulled reach the limit.
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
    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;

        /// Chunks pulled and not yet pushed, in order: data, then at most one virtual row.
        std::deque<Chunk> buffer;
        /// Rows and bytes of the data chunks in `buffer`: what the caps count.
        size_t buffered_rows = 0;
        size_t buffered_bytes = 0;

        /// See the class comment; empty until the first chunk.
        Columns bound;
        bool bound_is_virtual_row = false;

        /// Rows of data pulled since the start: the budget under a limit, and what closes the
        /// input once it reaches the limit.
        UInt64 rows_pulled = 0;

        bool in_set = false;
        /// May pull one block of data regardless of the window: granted to the lanes next in
        /// line when the demanded lane passes a fully filtered stretch.
        bool warmup = false;
        bool finished = false;

        bool ranked() const { return !bound.empty() && !finished; }
        /// The output can take a chunk and nothing is buffered: the merge is waiting on this lane.
        bool isDemanded() const;
        /// A virtual row waits at the tail of the buffer.
        bool holdsVirtualRow() const;
        bool underCaps(size_t max_rows, size_t max_bytes) const;
    };

    /// Orders lanes by (bound, lane index); the bounds live in `lanes`.
    struct BoundLess
    {
        const VirtualRowReadAheadTransform * self;
        bool operator()(size_t lhs, size_t rhs) const;
    };

    /// Who may read ahead, as decided from the ranking: the set, the frontier with the bound it
    /// had, and the two switches that gate the set. Two decisions compare equal when nothing that
    /// gates reading has changed between them.
    struct Decision
    {
        std::vector<size_t> set_lanes;
        ssize_t frontier_lane = -1;
        Columns frontier_bound;
        ssize_t demanded_lane = -1;
        bool window_open = false;
        bool budget_spent = false;
    };

    Status prepareImpl(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs);

    int compareKeys(const Columns & lhs, const Columns & rhs) const;
    Columns virtualRowKey(const Chunk & chunk) const;
    Columns lastRowKey(const Chunk & chunk) const;

    bool budgetSpent() const { return limit && budget_rows >= limit; }
    ssize_t frontierFor(size_t lane_num) const;
    bool passedFrontier(size_t lane_num) const;
    bool mayRead(size_t lane_num) const;
    bool sameDecision(const Decision & lhs, const Decision & rhs) const;
    bool chooseReaders();

    void serve(size_t lane_num);
    void pushReady(Lane & lane);
    void consume(size_t lane_num, Chunk chunk);
    void setBound(size_t lane_num, Columns key, bool is_virtual_row);
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

    /// Lanes with a known bound and an unfinished output, in (bound, lane index) order. An
    /// ordered set rather than a heap because a lane's bound moves with every chunk it pulls,
    /// so lanes are re-keyed all the time, and because the set and the frontier are the first
    /// K + 1 lanes of this order, which a heap cannot walk.
    std::set<size_t, BoundLess> ranked_lanes;
    /// The decision in force and the one before it; lanes in either are served after a change.
    Decision decision;
    Decision previous_decision;

    bool window_open;
    /// Rows pulled over lanes whose output is not finished.
    UInt64 budget_rows = 0;
    ssize_t first_demanded_lane = -1;
    /// The lane the merge asked for last: it keeps reading ahead like a set member.
    ssize_t demanded_lane = -1;
    size_t finished_lanes = 0;
    bool initialized = false;
};

}
