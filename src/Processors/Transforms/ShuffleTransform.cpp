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

void appendRowToReservoir(MutableColumns & reservoir_columns, std::vector<UInt64> & reservoir_priorities, const Chunk & source, size_t row, UInt64 priority)
{
    const auto & source_columns = source.getColumns();
    size_t num_columns = source_columns.size();

    for (size_t col = 0; col < num_columns; ++col)
        reservoir_columns[col]->insertFrom(*source_columns[col], row);

    reservoir_priorities.push_back(priority);
}

UInt64 getReservoirThreshold(const std::vector<UInt64> & reservoir_priorities)
{
    if (reservoir_priorities.empty())
        return std::numeric_limits<UInt64>::max();

    return *std::max_element(reservoir_priorities.begin(), reservoir_priorities.end());
}

size_t getMaxReservoirSize(size_t limit)
{
    if (limit > std::numeric_limits<size_t>::max() / 2)
        return std::numeric_limits<size_t>::max();

    return limit * 2;
}

void pruneReservoir(MutableColumns & reservoir_columns, std::vector<UInt64> & reservoir_priorities, size_t limit)
{
    if (reservoir_priorities.size() <= limit)
        return;

    std::vector<size_t> selected_indexes(reservoir_priorities.size());
    std::iota(selected_indexes.begin(), selected_indexes.end(), 0);

    std::nth_element(selected_indexes.begin(), selected_indexes.begin() + limit, selected_indexes.end(), [&](size_t lhs, size_t rhs)
    {
        return reservoir_priorities[lhs] < reservoir_priorities[rhs];
    });

    selected_indexes.resize(limit);
    std::sort(selected_indexes.begin(), selected_indexes.end());

    IColumn::Filter filter(reservoir_priorities.size(), 0);
    std::vector<UInt64> kept_priorities;
    kept_priorities.reserve(limit);
    for (size_t index : selected_indexes)
    {
        filter[index] = 1;
        kept_priorities.push_back(reservoir_priorities[index]);
    }

    for (auto & column : reservoir_columns)
        column = IColumn::mutate(column->filter(filter, static_cast<ssize_t>(limit)));

    reservoir_priorities.swap(kept_priorities);
}

void maybePruneReservoir(MutableColumns & reservoir_columns, std::vector<UInt64> & reservoir_priorities, UInt64 & reservoir_threshold, size_t limit)
{
    if (reservoir_priorities.size() < getMaxReservoirSize(limit))
        return;

    pruneReservoir(reservoir_columns, reservoir_priorities, limit);
    reservoir_threshold = getReservoirThreshold(reservoir_priorities);
}

void addSampledRow(
    MutableColumns & reservoir_columns,
    std::vector<UInt64> & reservoir_priorities,
    UInt64 & reservoir_threshold,
    size_t limit,
    const Chunk & chunk,
    size_t row,
    UInt64 priority)
{
    if (reservoir_priorities.size() >= limit && priority >= reservoir_threshold)
        return;

    appendRowToReservoir(reservoir_columns, reservoir_priorities, chunk, row, priority);

    if (reservoir_priorities.size() == limit)
        reservoir_threshold = getReservoirThreshold(reservoir_priorities);

    maybePruneReservoir(reservoir_columns, reservoir_priorities, reservoir_threshold, limit);
}

Chunk materializeSampledRows(MutableColumns & reservoir_columns, std::vector<UInt64> & reservoir_priorities, bool add_chunk_info)
{
    if (reservoir_priorities.empty())
        return {};

    IColumn::Permutation permutation(reservoir_priorities.size());
    std::iota(permutation.begin(), permutation.end(), 0);
    std::sort(permutation.begin(), permutation.end(), [&](size_t lhs, size_t rhs)
    {
        return reservoir_priorities[lhs] < reservoir_priorities[rhs];
    });

    Columns final_columns;
    final_columns.reserve(reservoir_columns.size());
    for (auto & column : reservoir_columns)
    {
        auto immutable = std::move(column);
        final_columns.push_back(immutable->permute(permutation, 0));
    }
    reservoir_columns.clear();

    std::vector<UInt64> priorities;
    priorities.reserve(reservoir_priorities.size());
    for (size_t index : permutation)
        priorities.push_back(reservoir_priorities[index]);
    reservoir_priorities.clear();

    Chunk result_chunk(std::move(final_columns), priorities.size());
    if (add_chunk_info)
        result_chunk.getChunkInfos().add(std::make_shared<ShuffleSamplingInfo>(std::move(priorities)));
    return result_chunk;
}

}

ShuffleTransform::ShuffleTransform(SharedHeader header_, size_t limit_)
    : IAccumulatingTransform(header_, header_)
    , limit(limit_)
{
    if (limit > 0)
    {
        reservoir_columns = header_->cloneEmptyColumns();
        reservoir_priorities.reserve(limit);
    }
}

void ShuffleTransform::consume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    if (limit > 0)
    {
        for (size_t row = 0; row < num_rows; ++row)
            addSampledRow(reservoir_columns, reservoir_priorities, reservoir_threshold, limit, chunk, row, rng());

        return;
    }

    total_rows += num_rows;
    accumulated.push_back(std::move(chunk));
}

Chunk ShuffleTransform::generate()
{
    if (limit > 0)
    {
        pruneReservoir(reservoir_columns, reservoir_priorities, limit);
        if (reservoir_priorities.empty())
            return {};

        return materializeSampledRows(reservoir_columns, reservoir_priorities, false);
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
    {
        reservoir_columns = header_->cloneEmptyColumns();
        reservoir_priorities.reserve(limit);
    }
}

void PartialShuffleTransform::consume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    for (size_t row = 0; row < num_rows; ++row)
        addSampledRow(reservoir_columns, reservoir_priorities, reservoir_threshold, limit, chunk, row, rng());
}

Chunk PartialShuffleTransform::generate()
{
    if (generated)
        return {};

    generated = true;
    pruneReservoir(reservoir_columns, reservoir_priorities, limit);
    return materializeSampledRows(reservoir_columns, reservoir_priorities, true);
}

MergingShuffleTransform::MergingShuffleTransform(SharedHeader header_, size_t limit_)
    : IAccumulatingTransform(header_, header_)
    , limit(limit_)
{
    if (limit > 0)
    {
        reservoir_columns = header_->cloneEmptyColumns();
        reservoir_priorities.reserve(limit);
    }
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

    for (size_t row = 0; row < num_rows; ++row)
        addSampledRow(reservoir_columns, reservoir_priorities, reservoir_threshold, limit, chunk, row, sampling_info->priorities[row]);
}

Chunk MergingShuffleTransform::generate()
{
    if (generated)
        return {};

    generated = true;
    pruneReservoir(reservoir_columns, reservoir_priorities, limit);
    Chunk result = materializeSampledRows(reservoir_columns, reservoir_priorities, false);
    result.setChunkInfos({});
    return result;
}

}
