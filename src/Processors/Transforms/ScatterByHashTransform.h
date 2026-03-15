#pragma once

#include <deque>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// Carries per-row hashes (shared across all shards from the same input chunk)
/// and the row-selector for one shard.
/// No need for cloneability — flows linearly through scatter -> resize -> aggregate.
struct ShardedChunkInfo : public ChunkInfo
{
    using KeyHashesPtr = std::shared_ptr<const PaddedPODArray<size_t>>;

    ShardedChunkInfo(KeyHashesPtr key_hashes_, IColumn::Selector row_indices_)
        : key_hashes(std::move(key_hashes_))
        , row_indices(std::move(row_indices_))
    {
    }

    Ptr clone() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "ShardedChunkInfo does not support cloning"); }

    KeyHashesPtr key_hashes; /// shared across all shards for the same input chunk
    IColumn::Selector row_indices; /// rows belonging to this shard
};

/// Scatters input rows to N output ports by hash(key) % N.
/// Materializes key and argument columns and computes per-row hashes once;
/// downstream ShardedAggregatingTransform builds its own aggregate instructions.
///
/// Only supports single-key GROUP BY.
///
/// Output ports can only accept one chunk at a time (canPush/push). But one input chunk
/// produces N output chunks (one per shard), and downstream aggregators consume them at
/// different rates. Without queueing, we would have to wait until all N outputs are ready
/// before splitting an input chunk — one slow shard would stall all others.
///
/// So each output port has a FIFO queue. When a shard's port is busy, its chunk waits in
/// the queue and gets pushed on the next prepare()/work() cycle. This allows other shards
/// to continue processing without waiting for the slowest one.
///
/// The queues are bounded (max_queued_chunks_per_shard) to prevent unbounded memory growth
/// if the source is faster than the aggregators. When any shard's queue is full, we stop
/// pulling new input — because each input chunk fans out to all shards, we cannot accept
/// a new chunk without overflowing the full queue. While waiting, we keep draining other
/// shards' queues. We only resume pulling once every queue is below the limit.
class ScatterByHashTransform : public IProcessor
{
public:
    ScatterByHashTransform(
        SharedHeader input_header, SharedHeader output_header, size_t num_shards_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "ScatterByHashTransform"; }

    Status prepare() override;
    void work() override;

private:
    void generateOutputChunks();

    size_t num_shards;
    AggregatingTransformParamsPtr params;

    /// Input chunk that was pulled in prepare() and will be split in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;

    /// Per-shard FIFO of chunks waiting to be pushed downstream.
    static constexpr size_t max_queued_chunks_per_shard = 4;
    std::vector<std::deque<Chunk>> output_queues;

    /// Cached to avoid per-chunk allocation in Aggregator::computeHashesForSharding.
    AggregatedDataVariants cached_hash_variants;
};

}
