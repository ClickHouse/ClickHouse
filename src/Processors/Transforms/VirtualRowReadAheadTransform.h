#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>
#include <unordered_map>

namespace DB
{

/// N-in / N-out processor between the in-order `MergeTree` streams (or their preliminary
/// merges) and the final `MergingSortedTransform` of a read-in-order query. Lane i connects
/// input i to output i. It takes over two things the merge used to get from elsewhere:
///
/// - the buffering of `BufferChunksTransform` and the one chunk the merge used to request
///   ahead into its input port: a lane keeps the next chunk ready and reads ahead up to the
///   caps, and after delivering a virtual row it waits until the merge asks for the lane again,
///   so a source parked behind its announcement is not read;
/// - the read-ahead window the merge itself used to run: lanes that start with a virtual row
///   are deferred, and up to `read_ahead_window` of them, nearest to the merge in key order,
///   are woken to read their next chunk (plus the buffering, if enabled), so their reads
///   overlap with the merge instead of running one at a time. A woken lane keeps its window
///   slot until the merge takes its data or asks for it and finds it exhausted. With a `LIMIT`
///   the window opens only after the merge has moved past a lane, since until then the front
///   lane alone may answer the query.
///
/// The window is refilled only when the merge asks for a lane, never on a source event: the
/// step in which the merge picks its next lane must not queue behind the sources it wakes.
/// The merge's own read-ahead is disabled for this (see `disableInputReadAhead`), so each of
/// its requests is a decision it took after consuming what it had.
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
    /// A lane's standing with the read-ahead window. Independent of `Lane::parked`, which is
    /// the merge's flow control: an announcement was delivered and the merge has not asked
    /// for the lane since. A deferred lane the merge asks for stays deferred until it delivers
    /// data: with per-block announcements it may answer with another virtual row, and the
    /// window must still be able to wake it.
    enum class Stage
    {
        Fresh,      /// no chunk seen yet
        Deferred,   /// announced itself, no data delivered yet; not woken by the window
        Prefetched, /// woken by the window; holds a slot until it delivers data or the merge finds it exhausted
        Active,     /// delivered data: resident for the merge's sake, not the window's
        Finished,
    };

    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;
        std::deque<Chunk> chunks;
        size_t buffered_rows = 0;
        size_t buffered_bytes = 0;
        UInt64 rows_read = 0;
        /// Sort key of the initial virtual row; empty for a lane that started with data.
        Columns key;
        Stage stage = Stage::Fresh;
        bool parked = false;
        bool delivered_data = false;
        UInt64 visited_pass = 0;
    };

    /// Pulls, pushes and finishes as far as the lane's ports allow. `asked` is true when the
    /// merge has just made the lane's output port ready for data.
    void processLane(size_t i, bool asked);
    /// Records the chunk in the lane's state; returns false for an empty chunk to drop.
    bool accept(Lane & lane, const Chunk & chunk);
    Columns extractKey(const Chunk & virtual_row) const;
    void setStage(Lane & lane, Stage stage);
    void finishLane(Lane & lane);
    /// Wakes deferred lanes in key order while fewer than `read_ahead_window` hold a slot.
    void wakeDeferredLanes();

    const SharedHeader header;
    const SortDescription description;
    const bool apply_virtual_row_conversions;
    const UInt64 limit;
    const size_t max_rows_to_buffer;
    const size_t max_bytes_to_buffer;
    const size_t read_ahead_window;
    std::vector<size_t> sort_positions;

    std::vector<Lane> lanes;
    std::unordered_map<const Port *, size_t> lane_of_port;
    size_t num_fresh;
    size_t num_finished = 0;
    size_t num_prefetching = 0;
    UInt64 pass = 0;

    /// Deferred lanes in the order the merge will reach them, fixed once every lane has
    /// announced itself; `next_deferred` walks it, so each lane is woken at most once.
    std::vector<size_t> deferred_order;
    bool deferred_order_built = false;
    size_t next_deferred = 0;
    /// The merge asked for a lane other than the one whose data it was consuming, or found
    /// the lane it asked for exhausted: with a `LIMIT`, this is what justifies reading ahead.
    bool merge_advanced = false;
    ssize_t last_lane_with_data = -1;
};

}
