#pragma once

#include <Core/Names.h>
#include <Processors/ISimpleTransform.h>

#include <vector>

namespace DB
{

class ColumnPermuteTransform : public ISimpleTransform
{
public:
    ColumnPermuteTransform(SharedHeader header_, const std::vector<size_t> & permutation_);

    String getName() const override { return "ColumnPermuteTransform"; }

    void transform(Chunk & chunk) override;

    static Block permute(const Block & block, const std::vector<size_t> & permutation);

private:
    Names column_names;
    std::vector<size_t> permutation;
};


}
