#pragma once

#include <deque>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Shards input rows to N output ports by hash(key) % N.
/// Hashes the key columns with WeakHash32 and physically splits every column with
/// IColumn::scatter so each output chunk holds only the rows belonging to its shard.
///
/// Output ports can only accept one chunk at a time (canPush/push). But one input chunk
/// produces N output chunks (one per shard), and downstream consume them at different rates.
/// Without queueing, we would have to wait until all N outputs are ready
/// before splitting an input chunk — one slow shard would stall all others.
///
/// So each output port has a FIFO queue. When a shard's port is busy, its chunk waits in
/// the queue and gets pushed on the next prepare()/work() cycle. This allows other shards
/// to continue processing without waiting for the slowest one.
///
/// TODO(nihalzp): A queue growing much faster than the others means the GROUP BY key
/// distribution is skewed onto one shard. That means one of the Aggregating hash tables would
/// be much bigger than the others, essentially serializing the pipeline. In that scenario, we could
/// potentially detect the skew from queue sizes and switch to a fallback where shard_i sends only to
/// AggregatingTransform_i % num_shards and then we merge at the end.
class BufferedShardByHashTransform : public IProcessor
{
public:
    BufferedShardByHashTransform(SharedHeader header, size_t num_shards_, ColumnNumbers key_columns_);

    String getName() const override { return "BufferedShardByHashTransform"; }

    Status prepare() override;
    void work() override;

private:
    void generateOutputChunks();

    /// Soft cap. Once any queue hits this length the transform stops pulling new input
    /// until the slow consumer drains it. The soft cap can be temporarily bypassed when
    /// a sibling output port has an empty queue and downstream demand on it - otherwise
    /// a sequential downstream consumer (e.g. `ConcatProcessor`) would deadlock waiting
    /// for data on the empty path while we back-pressure here. Under pathological hash
    /// skew (all rows hashing to one shard while a sibling port is asking), the bypass
    /// keeps growing that shard's queue until upstream is exhausted; memory is then
    /// bounded by `max_memory_usage`, not by this cap. A proper bound would require
    /// spilling overflow chunks; that is a separate follow-up.
    static constexpr size_t MAX_QUEUE_LENGTH = 10;

    size_t num_shards;
    ColumnNumbers key_columns;

    /// Input chunk that was pulled in prepare() and will be split in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;

    /// Per-shard FIFO of chunks waiting to be pushed downstream. Bounded at MAX_QUEUE_LENGTH.
    std::vector<std::deque<Chunk>> output_queues;

    /// Reused across input chunks to skip per-chunk reallocation.
    IColumn::Selector selector;
    std::vector<MutableColumns> shard_columns;
};

}
