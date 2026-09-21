#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>

namespace DB
{

/// N-in / N-out processor between the in-order `MergeTree` streams (or their preliminary
/// merges) and the final `MergingSortedTransform` of a read-in-order query. Lane i connects
/// input i to output i. It takes over two things the merge used to get from elsewhere:
///
/// - the buffering of `BufferChunksTransform`: an active lane reads ahead up to the caps, and
///   after delivering a virtual row it waits until the merge asks for the lane again, so a
///   source parked behind its announcement is not read;
/// - the read-ahead window the merge itself used to run: lanes that start with a virtual row
///   are deferred, and up to `read_ahead_window` of them, nearest to the merge in key order,
///   are woken once each, so their reads overlap with the merge instead of running one at a
///   time. With a `LIMIT` the window opens only after the merge has moved past a lane that
///   delivered data, since until then the front lane alone may answer the query.
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
    /// A lane's standing with the read-ahead window. Independent of `Lane::parked`, which is
    /// the merge's flow control: an announcement was delivered and the merge has not asked
    /// for the lane since.
    enum class Stage
    {
        Fresh,      /// no chunk seen yet
        Deferred,   /// announced itself; neither the merge nor the window has reached it
        Prefetched, /// woken by the window; holds one of its slots until data arrives or it runs dry
        Requested,  /// the merge asked for it after an announcement; no data delivered yet
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
    };

    /// Records the chunk in the lane's state; returns false for an empty chunk to drop.
    bool accept(Lane & lane, const Chunk & chunk);
    Columns extractKey(const Chunk & virtual_row) const;
    void finishLane(Lane & lane);
    /// Wakes deferred lanes in key order while fewer than `read_ahead_window` are prefetching.
    void wakeDeferredLanes(size_t prefetching);

    const SharedHeader header;
    const SortDescription description;
    const bool apply_virtual_row_conversions;
    const UInt64 limit;
    const size_t max_rows_to_buffer;
    const size_t max_bytes_to_buffer;
    const size_t read_ahead_window;
    std::vector<size_t> sort_positions;

    std::vector<Lane> lanes;

    /// Deferred lanes in the order the merge will reach them, fixed once every lane has
    /// announced itself; `next_deferred` walks it, so each lane is woken at most once.
    std::vector<size_t> deferred_order;
    bool deferred_order_built = false;
    size_t next_deferred = 0;
    /// The merge asked for a lane other than the one whose data it was consuming, or a lane
    /// it asked for ran dry: with a `LIMIT`, this is what justifies reading ahead.
    bool merge_advanced = false;
    ssize_t last_lane_with_data = -1;
};

}
