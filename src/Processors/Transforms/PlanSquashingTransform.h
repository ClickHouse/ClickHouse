#pragma once

#include <Interpreters/Squashing.h>
#include <Processors/IInflatingTransform.h>

namespace DB
{

class PlanSquashingTransform : public IInflatingTransform
{
public:
    PlanSquashingTransform(
        Block header_, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "PlanSquashingTransform"; }

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

