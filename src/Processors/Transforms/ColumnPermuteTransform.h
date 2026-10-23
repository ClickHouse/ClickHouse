#pragma once

#include <Core/Names.h>
#include <Processors/ISimpleTransform.h>

#include <vector>

namespace DB
{

class ColumnPermuteTransform final : public ISimpleTransform
{
public:
    ColumnPermuteTransform(SharedHeader header_, const std::vector<size_t> & permutation_);

    String getName() const override { return "ColumnPermuteTransform"; }

    /// The columns of a chunk on a device are permuted there, see `DeviceChunkInfo`.
    bool acceptsDeviceChunks() const override { return true; }

    void transform(Chunk & chunk) override;

    static Block permute(const Block & block, const std::vector<size_t> & permutation);

private:
    Names column_names;
    std::vector<size_t> permutation;
};


}
