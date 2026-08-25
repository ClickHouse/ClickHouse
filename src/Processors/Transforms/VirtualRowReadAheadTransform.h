#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <queue>
#include <unordered_map>

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
    /// The hot path: with N lanes and a chunk (or a virtual row) per wake, a full pass per
    /// event would put O(N) port inspections on the single-threaded critical path. Process
    /// only the lanes whose ports changed; fall back to the full pass for the first call and
    /// whenever a port finished (rare, and the full pass owns the termination bookkeeping).
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

        /// Granted read-ahead groups; a group is consumed by the next virtual row. Kept at
        /// most 1 deliberately, but note what that bounds: the window bounds how many lanes
        /// read at once, and the buffer caps bound how much *real data* a lane holds ahead of
        /// the merge. A lane whose groups are entirely filtered out keeps re-earning credit
        /// (via `topUpReadAhead` while it stays among the nearest boundaries, or the free-run
        /// below) and scans on: crossing those groups is work the merge would demand anyway,
        /// and vr0/vr1 read them just as eagerly; obsolete boundary announcements are
        /// collapsed in the buffer, so this costs one buffered virtual row, not an unbounded
        /// queue. A demand-paced lane re-earns its credit at every delivery, so a busy lane
        /// is not throttled. A lane that never produces virtual rows keeps its credit and
        /// streams like a plain bounded buffer.
        size_t credit = 0;

        /// Real rows seen since the last virtual row, and how many consecutive groups ended
        /// with none. While a filter discards entire groups, parking the lane at every group
        /// boundary buys nothing (the merge consumes the boundaries and comes right back) and
        /// only stalls the scan behind per-group wake-ups; after `free_run_dataless_groups`
        /// such groups the virtual rows stop consuming the credit and the lane free-runs like
        /// a single-shot one until real data appears.
        size_t rows_in_current_group = 0;
        size_t dataless_groups = 0;
    };

    /// See Lane::dataless_groups. The initial virtual row counts as the first one.
    static constexpr size_t free_run_dataless_groups = 2;

    /// Returns true if any port state changed (more progress may be possible).
    bool processLane(size_t lane_num);
    void onMiss(size_t lane_num);
    bool underBufferCaps(const Lane & lane) const
    {
        return lane.buffered_rows < max_rows_to_buffer || lane.buffered_bytes < max_bytes_to_buffer;
    }
    Status tryFinish();
    void grantCredit(size_t lane_num);
    void touchLane(size_t lane_num);
    void topUpReadAhead();
    bool speculationAllowed() const { return read_ahead_window > 0 && !has_collation; }
    bool boundaryLess(const Lane & lhs, const Lane & rhs) const;
    Columns extractBoundary(const Chunk & chunk) const;

    const SortDescription description;
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

    /// State of the partial `prepare`: lane lookup by port, dedup of touched lanes per call.
    std::unordered_map<const Port *, size_t> port_to_lane;
    std::vector<UInt64> lane_touch_epoch;
    std::vector<size_t> touched_lanes;
    UInt64 touch_epoch = 0;
    bool did_full_prepare = false;
};

}
