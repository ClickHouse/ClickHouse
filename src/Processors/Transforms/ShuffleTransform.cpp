#include <Processors/Transforms/ShuffleTransform.h>
#include <Processors/Port.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Common/PODArray.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <numeric>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct ShuffleSamplingInfo final : public ChunkInfoCloneable<ShuffleSamplingInfo>
{
    ShuffleSamplingInfo() = default;
    ShuffleSamplingInfo(const ShuffleSamplingInfo &) = default;
    explicit ShuffleSamplingInfo(std::vector<UInt64> priorities_)
        : priorities(std::move(priorities_))
    {
    }

    std::vector<UInt64> priorities;
};

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

template <typename Reservoir>
Chunk materializeSampledRows(Reservoir & reservoir, const Block & header)
{
    if (reservoir.empty())
        return {};

    std::vector<typename Reservoir::value_type> sampled_rows;
    sampled_rows.reserve(reservoir.size());

    for (auto & row : reservoir)
        sampled_rows.push_back(std::move(row));
    reservoir.clear();

    std::sort(sampled_rows.begin(), sampled_rows.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.priority < rhs.priority;
    });

    MutableColumns result = header.cloneEmptyColumns();
    size_t num_columns = result.size();

    for (const auto & sampled_row : sampled_rows)
    {
        for (size_t col = 0; col < num_columns; ++col)
            result[col]->insertFrom(*sampled_row.row.getColumns()[col], 0);
    }

    Columns final_columns;
    final_columns.reserve(num_columns);
    for (auto & col : result)
        final_columns.push_back(std::move(col));

    std::vector<UInt64> priorities;
    priorities.reserve(sampled_rows.size());
    for (const auto & sampled_row : sampled_rows)
        priorities.push_back(sampled_row.priority);

    Chunk result_chunk(std::move(final_columns), sampled_rows.size());
    result_chunk.getChunkInfos().add(std::make_shared<ShuffleSamplingInfo>(std::move(priorities)));
    return result_chunk;
}

template <typename Reservoir, typename Comparator>
void addSampledRow(Reservoir & reservoir, size_t limit, UInt64 priority, Chunk row, Comparator comparator)
{
    if (reservoir.size() < limit)
    {
        reservoir.push_back({priority, std::move(row)});
        std::push_heap(reservoir.begin(), reservoir.end(), comparator);
        return;
    }

    if (priority < reservoir.front().priority)
    {
        std::pop_heap(reservoir.begin(), reservoir.end(), comparator);
        reservoir.back() = {priority, std::move(row)};
        std::push_heap(reservoir.begin(), reservoir.end(), comparator);
    }
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

PartialShuffleTransform::PartialShuffleTransform(SharedHeader header_, size_t limit_)
    : IAccumulatingTransform(header_, header_)
    , limit(limit_)
{
    if (limit > 0)
        reservoir.reserve(limit);
}

void PartialShuffleTransform::consume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    SampledRowComparator comparator;
    for (size_t row = 0; row < num_rows; ++row)
        addSampledRow(reservoir, limit, rng(), copySingleRowChunk(chunk, row), comparator);
}

Chunk PartialShuffleTransform::generate()
{
    if (generated)
        return {};

    generated = true;
    return materializeSampledRows(reservoir, getOutputPort().getHeader());
}

MergingShuffleTransform::MergingShuffleTransform(SharedHeader header_, size_t limit_)
    : IAccumulatingTransform(header_, header_)
    , limit(limit_)
{
    if (limit > 0)
        reservoir.reserve(limit);
}

void MergingShuffleTransform::consume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto sampling_info = chunk.getChunkInfos().get<ShuffleSamplingInfo>();
    if (!sampling_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk in `MergingShuffleTransform` must have `ShuffleSamplingInfo`");

    if (sampling_info->priorities.size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`ShuffleSamplingInfo` size does not match number of rows in chunk");

    SampledRowComparator comparator;
    for (size_t row = 0; row < num_rows; ++row)
        addSampledRow(reservoir, limit, sampling_info->priorities[row], copySingleRowChunk(chunk, row), comparator);
}

Chunk MergingShuffleTransform::generate()
{
    if (generated)
        return {};

    generated = true;
    Chunk result = materializeSampledRows(reservoir, getOutputPort().getHeader());
    result.setChunkInfos({});
    return result;
}

}
