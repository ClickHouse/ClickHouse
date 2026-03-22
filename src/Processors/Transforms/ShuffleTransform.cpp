#include <Processors/Transforms/ShuffleTransform.h>
#include <Processors/Port.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <numeric>

namespace DB
{

namespace
{

Chunk copySingleRowChunk(const Chunk & source, size_t row)
{
    Columns row_columns;
    row_columns.reserve(source.getNumColumns());

    for (const auto & column : source.getColumns())
    {
        auto row_column = column->cloneEmpty();
        row_column->insertFrom(*column, row);
        row_columns.push_back(std::move(row_column));
    }

    return Chunk(std::move(row_columns), 1);
}

}

ShuffleTransform::ShuffleTransform(SharedHeader header_, size_t limit_)
    : IAccumulatingTransform(header_, header_)
    , limit(limit_)
{
    if (limit > 0)
        reservoir.reserve(limit);
}

void ShuffleTransform::consume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    if (limit > 0)
    {
        /// Reservoir sampling — Algorithm R (Vitter, 1985).
        /// Maintains a uniform random sample of k rows from the stream.
        /// Each row seen so far has equal probability min(k, rows_seen) / rows_seen
        /// of being in the reservoir.
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (total_rows < limit)
            {
                reservoir.push_back(copySingleRowChunk(chunk, row));
            }
            else
            {
                size_t j = rng() % (total_rows + 1);
                if (j < limit)
                    reservoir[j] = copySingleRowChunk(chunk, row);
            }
            ++total_rows;
        }

        return;
    }

    total_rows += num_rows;
    accumulated.push_back(std::move(chunk));
}

Chunk ShuffleTransform::generate()
{
    if (limit > 0)
    {
        if (reservoir.empty())
            return {};

        /// SHUFFLE LIMIT k — materialize the k sampled rows.
        size_t k = reservoir.size(); /// may be < limit if total_rows < limit

        /// Shuffle the reservoir for random output order
        /// (reservoir sampling gives a uniform *set*, but the order within
        /// the reservoir reflects replacement order — shuffle to randomize it)
        for (size_t i = k - 1; i > 0; --i)
        {
            size_t j = rng() % (i + 1);
            std::swap(reservoir[i], reservoir[j]);
        }

        MutableColumns result = getOutputPort().getHeader().cloneEmptyColumns();
        size_t num_columns = result.size();

        for (const auto & sampled_row : reservoir)
        {
            for (size_t col = 0; col < num_columns; ++col)
                result[col]->insertFrom(*sampled_row.getColumns()[col], 0);
        }

        reservoir.clear();

        Columns final_columns;
        final_columns.reserve(num_columns);
        for (auto & col : result)
            final_columns.push_back(std::move(col));

        return Chunk(std::move(final_columns), k);
    }

    if (accumulated.empty())
        return {};

    /// Full SHUFFLE (no limit) — Fisher-Yates on entire dataset
    MutableColumns merged_columns = getOutputPort().getHeader().cloneEmptyColumns();
    for (auto & chunk : accumulated)
    {
        auto columns = chunk.detachColumns();
        for (size_t i = 0; i < merged_columns.size(); ++i)
            merged_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
    }
    accumulated.clear();

    size_t n = total_rows;

    IColumn::Permutation permutation(n);
    std::iota(permutation.begin(), permutation.end(), 0);

    /// Fisher-Yates full shuffle: O(n)
    for (size_t i = n - 1; i > 0; --i)
    {
        size_t j = rng() % (i + 1);
        std::swap(permutation[i], permutation[j]);
    }

    Columns result_columns;
    result_columns.reserve(merged_columns.size());
    for (auto & col : merged_columns)
    {
        auto immutable = std::move(col);
        result_columns.push_back(immutable->permute(permutation, 0));
    }

    return Chunk(std::move(result_columns), n);
}

}
