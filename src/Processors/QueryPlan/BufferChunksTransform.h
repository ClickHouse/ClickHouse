#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <array>
#include <atomic>
#include <memory>
#include <queue>

namespace DB
{
class Block;

/// Shared by the `BufferChunksTransform`s in front of one read-in-order merge of a sort with a limit.
/// Inputs of such a merge usually do not overlap: when the first row of one input sorts before the first row of
/// another, all its rows sort before all rows of the other.
class ReadInOrderReadAheadTracker
{
public:
    /// A paused input keeps its reader open, so only merges with at most this many inputs use a tracker.
    static constexpr size_t max_inputs = 128;

    ReadInOrderReadAheadTracker(size_t num_inputs, const Block & header, const SortDescription & description_, size_t limit_);

    /// The limit of the sort. The merge itself has no limit when the sort is finished after it.
    size_t getLimit() const { return limit; }

    /// Called once per input, with its first chunk that has rows.
    void setFirstRow(size_t input, const Chunk & chunk);
    void setProducedRows(size_t input, size_t rows);

    /// Comparison results of `input` against the inputs in `published`, owned by the transform of `input`.
    struct InputsBefore
    {
        size_t num_compared = 0;
        VectorWithMemoryTracking<size_t> inputs;
    };

    /// Rows produced by the inputs in `published` whose first row sorts strictly before the first row of `input`.
    /// Requires `setFirstRow(input, ...)`.
    size_t rowsProducedBefore(size_t input, InputsBefore & inputs_before) const;

private:
    struct InputState
    {
        std::atomic<size_t> produced{0};
        Columns first_row; /// One row per sort column; written before the input enters `published`, read-only afterwards.
    };

    int compareFirstRows(size_t lhs, size_t rhs) const;

    const SortDescription description;
    const size_t limit;
    VectorWithMemoryTracking<size_t> sort_column_positions;
    VectorWithMemoryTracking<InputState> inputs; /// Sized in the constructor, never resized.
    /// `input + 1` for the inputs that called `setFirstRow`, in that order; 0 in a slot not written yet.
    std::array<std::atomic<size_t>, max_inputs> published{};
    std::atomic<size_t> num_published{0};
};

/// Transform that buffers chunks from the input
/// up to the certain limit  and pushes chunks to
/// the output whenever it is ready. It can be used
/// to increase parallelism of execution, for example
/// when it is adeded before MergingSortedTransform.
class BufferChunksTransform final : public IProcessor
{
public:
    /// OR condition is used for the limits on rows and bytes.
    BufferChunksTransform(
        SharedHeader header_,
        size_t max_rows_to_buffer_,
        size_t max_bytes_to_buffer_,
        size_t limit_,
        std::shared_ptr<ReadInOrderReadAheadTracker> read_ahead_tracker_ = nullptr,
        size_t input_index_ = 0);

    Status prepare() override;
    String getName() const override { return "BufferChunks"; }

private:
    Chunk pullChunk(bool & virtual_row);

    InputPort & input;
    OutputPort & output;

    size_t max_rows_to_buffer;
    size_t max_bytes_to_buffer;
    size_t limit;

    std::queue<Chunk> chunks;
    size_t num_buffered_rows = 0;
    size_t num_buffered_bytes = 0;
    size_t num_processed_rows = 0;

    /// After a virtual row was delivered downstream, the merge may defer this source
    /// (leave the port NotNeeded) until the merge reaches the key from the virtual row.
    /// While that is the case, do not read ahead into the buffer, otherwise buffering
    /// would defeat the deferral and every source would read speculatively regardless
    /// of `virtual_row_prefetch_window`. Without this flag the invariant only holds by
    /// an executor subtlety: the merge consumes the virtual row with `pull(true)`, which
    /// skips `updateVersion`, so nothing re-schedules this transform and the final
    /// `input.setNeeded()` of `prepare` is never reached. This flag makes the invariant
    /// explicit instead of relying on the wake-up pattern of the downstream processor.
    bool wait_for_demand_after_virtual_row = false;

    std::shared_ptr<ReadInOrderReadAheadTracker> read_ahead_tracker;
    size_t input_index = 0;
    ReadInOrderReadAheadTracker::InputsBefore inputs_before;
    bool first_row_published = false;
    /// The output port holds a chunk the merge has not taken yet.
    bool output_has_our_chunk = false;
    /// Chunks the merge took and then asked for more.
    size_t chunks_taken_by_merge = 0;
};

}
