#include <Processors/Transforms/ColumnPermuteTransform.h>

namespace DB
{

namespace
{

template <typename T>
void applyPermutation(std::vector<T> & data, const std::vector<size_t> & permutation)
{
    std::vector<T> res;
    res.reserve(permutation.size());
    for (size_t i = 0; i < permutation.size(); ++i)
        res.emplace_back(std::move(data[permutation[i]]));
    data = std::move(res);
}

Block permuteBlock(const Block & block, const std::vector<size_t> & permutation)
{
    auto columns = block.getColumnsWithTypeAndName();
    applyPermutation(columns, permutation);
    return Block(columns);
}

void permuteChunk(Chunk & chunk, const std::vector<size_t> & permutation)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    applyPermutation(columns, permutation);
    chunk.setColumns(std::move(columns), num_rows);
}

}

ColumnPermuteTransform::ColumnPermuteTransform(const Block & header_, std::vector<size_t> permutation_)
    : ISimpleTransform(header_, permuteBlock(header_, permutation_), false)
    , permutation(std::move(permutation_))
{
}


void ColumnPermuteTransform::transform(Chunk & chunk)
{
    permuteChunk(chunk, permutation);
}


}
