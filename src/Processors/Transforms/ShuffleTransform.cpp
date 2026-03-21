#include <Processors/Transforms/ShuffleTransform.h>
#include <Common/PODArray.h>
#include <Processors/Port.h>
#include <Columns/IColumn.h>
#include <pcg_random.hpp>
#include<Common/randomSeed.h>
#include <numeric>


namespace DB
{
ShuffleTransform::ShuffleTransform(SharedHeader header, size_t limit_)
    : IAccumulatingTransform(header, header)
    , limit(limit_)
{
}

void ShuffleTransform::consume(Chunk chunk)
{
    total_rows += chunk.getNumRows();
    accumulated.push_back(std::move(chunk)); 
}

Chunk ShuffleTransform::generate()
{
    if (accumulated.empty())
        return {};

    /// Concatenate all chunks into single columns
    MutableColumns merged_columns = getOutputPort().getHeader().cloneEmptyColumns();
    for (auto & chunk : accumulated)
    {
        auto columns = chunk.detachColumns();
        for (size_t i = 0; i < merged_columns.size(); ++i)
            merged_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
    }
    accumulated.clear();

    /// Build permutation
    size_t n = total_rows;
    size_t k = (limit > 0 && limit < n) ? limit : n;

    IColumn::Permutation permutation(n);
    std::iota(permutation.begin(), permutation.end(), 0);

    /// Fisher-Yates partial shuffle: O(k)
    pcg64 rng(randomSeed());
    for (size_t i = 0; i < k; ++i)
    {
        size_t j = i + rng() % (n - i);
        std::swap(permutation[i], permutation[j]);
    }


    Columns result_columns;
    result_columns.reserve(merged_columns.size());
    for (auto & col : merged_columns)
    {
        auto immutable = std::move(col);
        result_columns.push_back(immutable->permute(permutation, k));
    }

    return Chunk(std::move(result_columns), k);
}

}
