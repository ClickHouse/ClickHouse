#pragma once

#include <Common/DequeWithMemoryTracking.h>
#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ApplySquashingTransform.h>

namespace DB
{

class QueryPipelineBuilder;

class SquashingTransform final : public ExceptionKeepingTransform
{
public:
    explicit SquashingTransform(
        SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes,
        size_t max_block_size_rows = 0, size_t max_block_size_bytes = 0, bool squash_with_strict_limits = false);

    String getName() const override { return "SquashingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    bool canGenerate() override;
    void onFinish() override;

private:
    Squashing squashing;
    Chunk cur_chunk;
    Chunk finish_chunk;
};

class SimpleSquashingChunksTransform final : public IInflatingTransform
{
public:
    /// Non-zero max bounds stop merging before the merged chunks' total rows or bytes would exceed them; a single chunk is never split.
    /// The byte bound is an estimate: it sums the input chunks, and concatenation can change a column's representation.
    explicit SimpleSquashingChunksTransform(
        SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes,
        size_t max_block_size_rows_ = 0, size_t max_block_size_bytes_ = 0);

    String getName() const override { return "SimpleSquashingTransform"; }

    /// Preview chunks pass through untouched (see `IInflatingTransform::prepare`).
    bool supportsQueryResultPreviews() const override { return true; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    Squashing squashing;
    Chunk squashed_chunk;

    const size_t max_block_size_rows;
    const size_t max_block_size_bytes;
    /// `squashedBytes` of the chunks buffered in `squashing`; valid only while it is non-empty.
    size_t buffered_bytes = 0;
    /// Buffered data emitted ahead of a chunk that would overflow a max bound.
    Chunk flushed_chunk;
};

/// Like `SimpleSquashingChunksTransform`, but for a consumer that takes the rows as they come:
///  - it keeps at most as many rows as the stream already sent, so a consumer that needs only a few rows
///    does not wait for a full chunk;
///  - a chunk of aggregation states is not combined, because its bucket number is for the whole chunk;
///  - a chunk without rows passes, because it is a marker for the consumer.
/// Put it ahead of a point that divides the stream between consumers: behind it, a consumer that does
/// not read would hold back the rows that the transform keeps for the others.
class ProgressiveSquashingChunksTransform final : public IInflatingTransform
{
public:
    /// A chunk is complete at `min_block_size_rows` rows or `min_block_size_bytes` bytes. A zero limit is not checked.
    ProgressiveSquashingChunksTransform(SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes_);

    String getName() const override { return "ProgressiveSquashingTransform"; }

    /// The limit in bytes for a caller that has only a limit in rows.
    static constexpr size_t DEFAULT_MIN_BLOCK_SIZE_BYTES = 1024 * 1024;

    /// Puts the transform on every stream of the pipeline. Does nothing when both limits are zero.
    static void addToPipeline(
        QueryPipelineBuilder & pipeline, size_t min_block_size_rows, size_t min_block_size_bytes = DEFAULT_MIN_BLOCK_SIZE_BYTES);

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    /// Moves to `ready_chunks` the chunks that reached a limit, or with `all_rows` all that `squashing` has.
    void takeSquashedChunks(bool all_rows);

    Squashing squashing;
    const size_t min_block_size_bytes;
    /// Size of the held chunks after `Squashing::squash`, which makes a constant column full.
    size_t held_bytes = 0;
    /// Rows that left this transform in combined chunks.
    size_t squashed_rows_sent = 0;
    DequeWithMemoryTracking<Chunk> ready_chunks;
};

}
