#pragma once

#include <Processors/ISimpleTransform.h>
#include <DataStreams/SquashingTransformer.h>

namespace DB
{

class SquashingTransform : public ISimpleTransform
{
public:
    void transform(Chunk & chunk) override;

    String getName() const override { return "SquashingTransform"; }

    SquashingTransform(const Block & header_, size_t min_block_size_rows_, size_t min_block_size_bytes_);

private:
    Block header;
    SquashingTransformer transformer;

};

}
