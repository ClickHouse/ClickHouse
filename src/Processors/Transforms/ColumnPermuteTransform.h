#pragma once

#include <Core/Names.h>
#include <Processors/ISimpleTransform.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class ColumnPermuteTransform final : public ISimpleTransform
{
public:
    ColumnPermuteTransform(SharedHeader header_, const VectorWithMemoryTracking<size_t> & permutation_);

    String getName() const override { return "ColumnPermuteTransform"; }

    void transform(Chunk & chunk) override;

    static Block permute(const Block & block, const VectorWithMemoryTracking<size_t> & permutation);

private:
    Names column_names;
    VectorWithMemoryTracking<size_t> permutation;
};


}
