#pragma once

#include <Columns/IColumn.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Shards the rows of one sorted input stream to `num_shards` output ports by hash(key) % `num_shards`, for a
/// reshuffle in which every shard is consumed by a `MergingSortedTransform` that merges the shard's pieces from
/// all input streams back into one sorted stream (see shuffled aggregation-in-order in `AggregatingStep`).
/// Hashes the key columns with `IColumn::computeHashInto` and physically splits every column with
/// `IColumn::scatter`, so each output chunk holds only the rows belonging to its shard.
///
/// Memory is bounded with ordinary back-pressure: every shard has a queue of pieces that are not pushed yet, at
/// most one per input chunk, and a new input chunk is read only while every queue is shorter than
/// `max_pending_pieces`. So a scatter holds at most `max_pending_pieces` input chunks, plus the chunk parked in
/// each output port.
///
/// Back-pressure alone would deadlock here, because a sorted merge is a selective consumer: it cannot emit
/// anything until it has a head row from every input, so it can stop pulling from one scatter while it waits
/// for another. Example: stream 1 is in a long run of a key of shard A, so scatter 1 blocks on A's full port;
/// merge A waits for stream 2's next row of shard A, which is behind many rows of shard B; scatter 2 blocks on
/// B's full port; and merge B waits for stream 1 to pass the run, which it can only do once merge A consumes it.
///
/// The cycle is broken with progress markers. Every input row is followed by rows that are not smaller in the
/// sort order, so once a chunk is read, its last row is a lower bound for everything this stream can still
/// send to any shard. The scatter tells it to a shard that is waiting for this stream with a virtual row
/// (`MergeTreeReadInfo` with a `pk_block`), which the merge compares like a real row but never outputs, and
/// which lets the merge advance its other inputs up to that bound. A marker is sent to an output that can
/// accept a chunk and has nothing queued, when either the shard received no rows of the current chunk, or the
/// scatter cannot read further because another shard's queue is full. After pulling a marker the merge does
/// not ask for the input again until it has consumed the marker, so markers are only produced on demand.
///
/// Why this cannot deadlock: all merges break ties between equal sort keys by input number, so they all order
/// rows by the same total order (key, stream, position in stream). Suppose every scatter and merge is stuck.
/// A stuck merge Y waits for an input from a scatter k that pushed everything it had for Y, including the bound
/// of its current chunk (the last row of the chunk or a marker), so all heads of Y are above that bound E(k).
/// Scatter k is stuck on a full queue of some merge Z, whose port is full too, so Z still holds a head from an
/// earlier chunk of k than the pieces in the queue, and that head is below E(k). Z is in turn stuck on some
/// scatter k' with E(k') below that head. Following the cycle, E(k) > E(k') > ... > E(k), a contradiction.
class BufferedShardByHashTransform : public IProcessor
{
public:
    /// `sorting_columns` are the columns the downstream merges sort by; markers carry only these columns.
    BufferedShardByHashTransform(
        SharedHeader header,
        size_t num_shards_,
        ColumnNumbers key_columns_,
        ColumnNumbers sorting_columns_);

    String getName() const override { return "BufferedShardByHashTransform"; }

    Status prepare() override;
    void work() override;

private:
    void splitInputChunk();
    Chunk makeMarker() const;

    size_t num_shards;
    ColumnNumbers key_columns;
    ColumnNumbers sorting_columns;

    /// Input chunk that was pulled in prepare() and will be split in work().
    Chunk input_chunk;
    bool has_input_chunk = false;

    /// Pieces of the input chunks that are not pushed yet, for each shard, at most one per input chunk. A new input
    /// chunk is read only while every queue has less than `max_pending_pieces` of them.
    struct Piece
    {
        Chunk chunk;
        size_t chunk_number = 0;
    };
    VectorWithMemoryTracking<DequeWithMemoryTracking<Piece>> queues;
    /// More than one piece of slack lets the scatter run ahead of a merge that is briefly behind (e.g. descheduled),
    /// instead of stalling every other shard in lockstep with it. With markers, the pieces rarely accumulate, so a
    /// larger value costs little memory in practice: on parts with a single key each, 16 was as fast as 64 and ten
    /// times faster than 1, with the same peak memory.
    static constexpr size_t max_pending_pieces = 16;
    /// The number of the last input chunk that was split.
    size_t current_chunk_number = 0;
    /// Whether the shard got rows from the current input chunk.
    VectorWithMemoryTracking<char> has_rows_in_current_chunk;
    /// Whether the shard has been sent the bound of the current input chunk: the piece that ends with the last
    /// row of the chunk, or a marker.
    VectorWithMemoryTracking<char> bound_sent;
    /// The shard of the last row of the current input chunk.
    size_t last_row_shard = 0;
    /// The sort columns of the last row of the current input chunk; empty before the first chunk.
    Block bound;

    /// Reused across input chunks to skip per-chunk reallocation.
    PaddedPODArray<UInt32> hash_buffer;
    IColumn::Selector selector;
};

}
