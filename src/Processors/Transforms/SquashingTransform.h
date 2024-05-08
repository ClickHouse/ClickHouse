#pragma once

#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
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

/// Doesn't care about propagating exceptions and thus doesn't throw LOGICAL_ERROR if the following transform closes its input port.
class SimpleSquashingTransform : public ISimpleTransform
{
public:
    explicit SimpleSquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "SimpleSquashingTransform"; }

protected:
    void transform(Chunk &) override;

    IProcessor::Status prepare() override;

private:
    Squashing squashing;

    bool finished = false;
};
}
