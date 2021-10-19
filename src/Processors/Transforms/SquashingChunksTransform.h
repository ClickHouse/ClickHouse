#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <DataStreams/SquashingTransform.h>

namespace DB
{

class SquashingChunksTransform : public IAccumulatingTransform
{
public:
    explicit SquashingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_memory = false);

    String getName() const override { return "SquashingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    SquashingTransform squashing;
};

}
