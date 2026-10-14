#pragma once

#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ApplySquashingTransform.h>

#include <memory>
#include <optional>

namespace DB
{

class IJoin;

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

class SimpleSquashingChunksTransform : public IInflatingTransform
{
public:
    /// Non-zero max bounds stop merging before the merged chunks' total rows or bytes would exceed them; a single chunk is never split.
    /// The byte bound is an estimate: it sums the input chunks, and concatenation can change a column's representation.
    explicit SimpleSquashingChunksTransform(
        SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes,
        size_t max_block_size_rows_ = 0, size_t max_block_size_bytes_ = 0);

    String getName() const override { return "SimpleSquashingTransform"; }

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

/// Squashes the output of a join whose inputs were widened per slot (`IJoin::supportParallelJoin`). Such a
/// join emits one slot's fragment of every probe block, and downstream wants full blocks back. A join
/// that already caps its output at `max_joined_block_size_*` would only pay one more copy per row, so its
/// chunks pass through. The join answers on the first chunk (`IJoin::emitsSizedOutputBlocks`), because the
/// spilling wrapper knows only after the build whether it kept the in-memory join or switched to grace.
/// The transform keeps the name of the one it replaces, so pipeline dumps do not change.
class JoinOutputSquashingTransform final : public SimpleSquashingChunksTransform
{
public:
    JoinOutputSquashingTransform(
        SharedHeader header,
        size_t min_block_size_rows,
        size_t min_block_size_bytes,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        std::shared_ptr<IJoin> join_);

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    std::shared_ptr<IJoin> join;
    /// Decided on the first chunk: the build is over by then, so the join's answer is final.
    std::optional<bool> pass_through;
    Chunk passed_chunk;
};

}
