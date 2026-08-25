#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <queue>

namespace DB
{

/// Sits between in-order MergeTree sources (which announce their position with virtual rows)
/// and a MergingSortedTransform, owning both the per-lane buffering and the read-ahead policy
/// for sources deferred behind virtual rows.
///
/// A virtual row — the first chunk of a source and, with `read_in_order_use_virtual_row_per_block`,
/// a chunk after every block — carries the sort key of the source's next output. The merge uses it
/// as a cursor and does not demand real data from the source until the merge reaches that key, so
/// sources the merge never reaches are never read. Left alone, that demand pattern serializes reads:
/// every source waits for the merge to come back to it. This transform restores parallelism while
/// keeping the read savings:
///
/// - A lane is read only on downstream demand ("miss": the merge asks, the buffer is empty), so a
///   `LIMIT` answered by the front source alone reads nothing else.
/// - A demanded lane is kept one group ahead (a group = chunks up to and including the next virtual
///   row), overlapping its next read with the merge of the current block.
/// - Once demand has moved across lanes at least once (or there is no limit), up to
///   `read_ahead_window` parked lanes closest to the merge in key order read one group ahead in
///   parallel, so a scan over many sources is not read one source at a time.
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

        /// Granted read-ahead groups; a group is consumed by the next virtual row. Kept at
        /// most 1 deliberately: one group is the per-lane speculation depth, so when a limit
        /// finishes the merge early, the waste is bounded by one group per lane in the window
        /// (the window bounds how many lanes read at once, the credit bounds how deep each
        /// goes). A demand-paced lane re-earns its credit at every delivery, so a busy lane
        /// is not throttled by this. Deeper credit is a possible follow-up for the per-block
        /// mode, where a scan pays a wake-up per group. A lane that never produces virtual
        /// rows keeps its credit and streams like a plain bounded buffer.
        size_t credit = 0;
    };

    /// Returns true if any port state changed (more progress may be possible).
    bool processLane(size_t lane_num);
    void onMiss(size_t lane_num);
    bool underBufferCaps(const Lane & lane) const
    {
        return lane.buffered_rows < max_rows_to_buffer || lane.buffered_bytes < max_bytes_to_buffer;
    }
    void grantCredit(Lane & lane);
    void topUpReadAhead();
    bool speculationAllowed() const { return read_ahead_window > 0 && !has_collation; }
    bool boundaryLess(const Lane & lhs, const Lane & rhs) const;
    Columns extractBoundary(const Chunk & chunk) const;

    SharedHeader header;
    const SortDescription description;
    std::vector<size_t> sort_column_positions;
    const bool apply_virtual_row_conversions;
    const UInt64 limit;
    const size_t max_rows_to_buffer;
    const size_t max_bytes_to_buffer;
    const size_t read_ahead_window;
    /// With a collated sort description the boundary comparison in `boundaryLess` would need
    /// collator-aware comparison, so speculation is disabled and lanes are read strictly on
    /// demand — same as the read-ahead window the merge used to have (it was also guarded by
    /// `!has_collation`). In practice this path is not reachable: virtual rows come from
    /// reading in the binary order of the primary key, which a collated ORDER BY does not
    /// follow, so read-in-order does not apply there; the guard is defensive.
    bool has_collation = false;

    std::vector<Lane> lanes;

    /// Read-ahead beyond the demanded lane starts only after demand has visited two distinct
    /// lanes (or with no limit): until then the limit may be answered by the front lane alone
    /// and the other lanes must stay unread.
    ssize_t first_miss_lane = -1;
    bool cross_lane_read_ahead = false;
};

}
