#pragma once

#include <Interpreters/Squashing.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

namespace DB
{

class PlanSquashingTransform : public ExceptionKeepingTransform
{
public:
    PlanSquashingTransform(
        SharedHeader header_, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "PlanSquashingTransform"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

    bool canGenerate() override;
    GenerateResult getRemaining() override;

private:
    Squashing squashing;
    Chunk squashed_chunk;
};
}
