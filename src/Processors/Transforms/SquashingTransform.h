#pragma once

#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ApplySquashingTransform.h>

namespace DB
{

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

}
