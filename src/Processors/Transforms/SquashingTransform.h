#pragma once

#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ApplySquashingTransform.h>

namespace DB
{

class SquashingTransform : public ExceptionKeepingTransform
{
public:
    explicit SquashingTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "SquashingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    void onFinish() override;

private:
    Squashing squashing;
    Chunk cur_chunk;
    Chunk finish_chunk;
};

class SimpleSquashingChunksTransform : public IInflatingTransform
{
public:
    explicit SimpleSquashingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

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

}
