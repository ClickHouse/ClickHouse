#pragma once

#include <deque>
#include <Columns/IColumn.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>

namespace DB
{

/// Implementation for LIMIT -N OFFSET -M (drops last M rows, then gives last N rows)
/// This processor supports multiple inputs and outputs (the same number).
/// The output ports are interchangeable, chunks can be pushed to any available output.
/// The reason to have multiple ports is to be able to stop all sources when limit is reached, in a query like:
///     SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT -1
///
/// This processor also supports WITH TIES, which means that if the last row of the limit window is tied with following rows,
/// those are included in the output as well.
/// For WITH TIES, there must be a single input stream, and a non-empty SortDescription must be provided to determine the tie key.
///
/// For WITH TIES, we use the following definitions:
///
///   Given sorted data [0 .. total-1], offset=M, limit=N:
///
///   - Offset tail: the last M rows [total-M .. total-1], always discarded from output.
///   - Limit window: the N rows before the offset tail [total-M-N .. total-M-1].
///   - Boundary: the first row of the limit window (row total-M-N). This is the row
///     whose sort key determines tie extension.
///   - Tie-run: the maximal contiguous range of rows with the same sort key as the
///     boundary. It extends leftward from the boundary (and possibly rightward, but
///     the rightward part is already inside the limit window).
///   - Output window: tie-run + limit window. This is what gets pushed to output.
///     It is >= the limit window because ties can only extend it.
///
/// During Pull, we track the boundary and the start of its tie-run.
/// Chunks before the tie-run start are discarded immediately. This gives
/// optimal memory usage at chunk granularity with O(1) amortized work per chunk.
class NegativeLimitTransform final : public IProcessor
{
private:
    UInt64 limit;
    UInt64 offset;

    bool with_ties;
    const SortDescription description;
    std::vector<size_t> sort_column_positions;

    /// Total rows currently queued across all inputs.
    UInt64 queued_row_count = 0;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of a port pair.
    struct PortsData
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_port_finished = false;

        /// This flag is used to avoid counting rows multiple times before applying a limit
        /// condition, which can happen through certain input ports like PartialSortingTransform and
        /// RemoteSource.
        bool input_port_has_counter = false;
    };

    UInt64 num_input_ports_finished = 0;

    std::vector<PortsData> ports_data;

    /// `Pull` stage: it ends when all input ports are closed.
    /// `Push` stage: it starts immediately after the `Pull` stage and it ends
    ///               when all queued full/partial chunks within limit are pushed
    ///               to output ports excluding the offset.
    enum class Stage : uint8_t
    {
        Pull = 0,
        Push
    };

    Stage stage = Stage::Pull;

    size_t next_output_port = 0;

    /// Stores the pending chunks. With ties, chunks before the tie-run start are
    /// discarded during Pull. A deque is used for indexed access during boundary tracking.
    std::deque<Chunk> chunks;

    /// WITH TIES boundary tracking state.
    /// The boundary is the first row of the limit window.
    /// The tie-run may extend leftward from the boundary;
    /// `tie_run_start_chunk_idx` tracks the earliest chunk it reaches.
    struct TiesTrackingState
    {
        bool has_excess = false; /// true once total rows exceeded limit + offset and tracking began
        size_t limit_boundary_chunk_idx = 0; /// chunk containing the limit boundary row
        UInt64 limit_boundary_row_idx = 0; /// row index of the limit boundary within that chunk
        size_t tie_run_start_chunk_idx = 0; /// earliest chunk in the tie-run of the limit boundary
    };

    TiesTrackingState boundary;

public:
    NegativeLimitTransform(
        SharedHeader header_,
        UInt64 limit_,
        UInt64 offset_,
        size_t num_streams = 1,
        bool with_ties_ = false,
        SortDescription description_ = {});

    String getName() const override { return "NegativeLimit"; }

    Status prepare() override;

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
    void setInputPortHasCounter(size_t pos) { ports_data[pos].input_port_has_counter = true; }

private:
    /// Process a single input port, populates `chunks`.
    Status advancePort(size_t pos);

    /// Find an output port that can accept data at the moment.
    OutputPort * getAvailableOutputPort();

    bool allOutputsFinished() const;

    /// True when there are rows before the limit window, i.e. queued_row_count > limit + offset.
    bool hasExcessBeforeLimitWindow() const { return queued_row_count > offset && queued_row_count - offset > limit; }

    /// Position of the boundary row from the front of the deque.
    /// The boundary is the first row of the limit window (limit rows before the offset tail).
    /// The output window may be larger due to tie extension backward.
    /// Example: 12 rows, limit=3, offset=2. The deque is [0..11], the limit window is
    /// rows [7..9], the offset tail is [10..11]. Boundary = row 7, position from front = 12-2-3 = 7.
    /// Only valid when `hasExcessBeforeLimitWindow` is true.
    UInt64 boundaryPosFromFront() const
    {
        chassert(hasExcessBeforeLimitWindow());
        return (queued_row_count - offset) - limit;
    }

    /// Check if two rows have equal sort keys by comparing all ORDER BY columns.
    bool sortKeysEqual(const Chunk & lhs, UInt64 lhs_row, const Chunk & rhs, UInt64 rhs_row) const;

    /// Advance the boundary by `delta` rows to the right within the deque.
    /// Called when new rows arrive: the limit window shifts right, so the boundary moves too.
    void advanceLimitBoundary(UInt64 delta);

    /// Scan left from boundary to find the earliest chunk in its tie-run.
    void findRunStartChunk();

    /// Discard all chunks before the tie-run start.
    void discardChunksBeforeRunStart();

    /// After all data is pulled, find the exact row in the front
    /// chunk where the tie-run begins, and cut away the rows before it.
    void cutFrontChunkToRunStart();

    /// Push phase: push `to_push` rows from the front of the deque to output ports.
    /// Returns PortFull if no output port can accept data.
    Status pushRows(UInt64 to_push);

    /// Debug: verify that `queued_row_count` equals the sum of all chunk row counts.
    void assertRowCountConsistency() const
    {
        [[maybe_unused]] UInt64 actual = 0;
        for (const auto & c : chunks)
            actual += c.getNumRows();
        chassert(actual == queued_row_count);
    }
};

}
