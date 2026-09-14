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

class SimpleSquashingChunksTransform final : public IInflatingTransform
{
public:
    explicit SimpleSquashingChunksTransform(SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "SimpleSquashingTransform"; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    Squashing squashing;
    Chunk squashed_chunk;
};

/// The squashing after a join whose inputs were widened per slot (`IJoin::supportParallelJoin`). Such a
/// join emits one slot's fragment of every probe block, and downstream wants full blocks back. A join
/// that already caps its output at `max_joined_block_size_*` gains nothing from it and would pay one
/// more copy of every output row, so its chunks pass through. The join answers at run time
/// (`IJoin::emitsSizedOutputBlocks`): the spilling wrapper only knows after the build whether it kept
/// the in-memory join or switched to grace, whose per-bucket output does need the squashing.
/// Keeps the name of the transform it replaces so pipeline dumps do not change.
class JoinOutputSquashingTransform final : public IInflatingTransform
{
public:
    JoinOutputSquashingTransform(SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes, std::shared_ptr<IJoin> join_);

    String getName() const override { return "SimpleSquashingTransform"; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    std::shared_ptr<IJoin> join;
    /// Decided on the first chunk: the build is over by then, so the join's answer is final.
    std::optional<bool> pass_through;
    Squashing squashing;
    Chunk squashed_chunk;
    Chunk passed_chunk;
    bool passed_pending = false;
};

}
