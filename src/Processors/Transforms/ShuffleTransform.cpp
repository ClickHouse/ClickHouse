#include <Processors/Transforms/ShuffleTransform.h>

#include <cmath>
#include <numeric>
#include <random>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/randomSeed.h>

namespace DB
{

ShuffleTransform::ShuffleTransform(SharedHeader header, std::optional<size_t> limit_)
    : IAccumulatingTransform(header, header)
    , limit(limit_)
    , rng(randomSeed())
{
}

// ---- consume -----------------------------------------------------------

void ShuffleTransform::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    if (limit.has_value())
        consumeReservoir(std::move(chunk));
    else
        accumulated.push_back(std::move(chunk));
}

// ---- Algorithm L reservoir sampling ------------------------------------

void ShuffleTransform::initAlgorithmL()
{
    const double k = static_cast<double>(*limit);
    std::uniform_real_distribution<double> uniform(std::nextafter(0.0, 1.0), 1.0);
    w = std::exp(std::log(uniform(rng)) / k);
    size_t skip = static_cast<size_t>(std::floor(std::log(uniform(rng)) / std::log(1.0 - w)));
    next_replace = *limit + skip;
}

void ShuffleTransform::advanceAlgorithmL(size_t current_index)
{
    const double k = static_cast<double>(*limit);
    std::uniform_real_distribution<double> uniform(std::nextafter(0.0, 1.0), 1.0);
    w *= std::exp(std::log(uniform(rng)) / k);
    size_t skip = static_cast<size_t>(std::floor(std::log(uniform(rng)) / std::log(1.0 - w)));
    next_replace = current_index + skip + 1;
}

void ShuffleTransform::consumeReservoir(Chunk chunk)
{
    const size_t k = *limit;
    if (k == 0)
        return;

    const size_t num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    const size_t num_cols = columns.size();

    for (size_t row = 0; row < num_rows; ++row)
    {
        const size_t current_index = rows_seen;
        ++rows_seen;

        if (current_index < k)
        {
            // Phase 1: fill the reservoir sequentially.
            ReservoirSlot slot;
            slot.columns.resize(num_cols);
            for (size_t col = 0; col < num_cols; ++col)
            {
                slot.columns[col] = columns[col]->cloneEmpty();
                slot.columns[col]->insertFrom(*columns[col], row);
            }
            reservoir_slots.push_back(std::move(slot));

            if (rows_seen == k)
                initAlgorithmL();
        }
        else
        {
            // Phase 2: Algorithm L — skip rows until next_replace is reached.
            if (current_index == next_replace)
            {
                size_t replace_idx = rng() % k;
                ReservoirSlot & slot = reservoir_slots[replace_idx];
                for (size_t col = 0; col < num_cols; ++col)
                {
                    slot.columns[col] = columns[col]->cloneEmpty();
                    slot.columns[col]->insertFrom(*columns[col], row);
                }
                advanceAlgorithmL(current_index);
            }
        }
    }
}

// ---- generate ----------------------------------------------------------

Chunk ShuffleTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (limit.has_value())
        return generateReservoir();
    else
        return generateFullShuffle();
}

Chunk ShuffleTransform::generateReservoir()
{
    if (reservoir_slots.empty())
        return {};

    const size_t k = reservoir_slots.size();
    const size_t num_cols = reservoir_slots[0].columns.size();

    // Concatenate all reservoir slots into flat columns.
    MutableColumns merged(num_cols);
    for (size_t col = 0; col < num_cols; ++col)
    {
        merged[col] = reservoir_slots[0].columns[col]->cloneEmpty();
        merged[col]->reserve(k);
        for (size_t slot = 0; slot < k; ++slot)
            merged[col]->insertRangeFrom(*reservoir_slots[slot].columns[col], 0, 1);
    }
    reservoir_slots.clear();

    // Apply Fisher-Yates to produce a uniformly random ordering.
    IColumn::Permutation perm(k);
    std::iota(perm.begin(), perm.end(), 0);
    for (size_t i = k - 1; i > 0; --i)
    {
        std::uniform_int_distribution<size_t> dist(0, i);
        std::swap(perm[i], perm[dist(rng)]);
    }

    Columns result(num_cols);
    for (size_t col = 0; col < num_cols; ++col)
        result[col] = merged[col]->permute(perm, 0);

    return Chunk(std::move(result), k);
}

Chunk ShuffleTransform::generateFullShuffle()
{
    if (accumulated.empty())
        return {};

    size_t total_rows = 0;
    for (const auto & chunk : accumulated)
        total_rows += chunk.getNumRows();

    if (total_rows == 0)
        return {};

    const size_t num_cols = accumulated.front().getNumColumns();

    // Merge all chunks into a single set of mutable columns.
    MutableColumns merged(num_cols);
    for (size_t col = 0; col < num_cols; ++col)
    {
        merged[col] = accumulated.front().getColumns()[col]->cloneEmpty();
        merged[col]->reserve(total_rows);
    }
    for (auto & chunk : accumulated)
    {
        auto columns = chunk.detachColumns();
        for (size_t col = 0; col < num_cols; ++col)
            merged[col]->insertRangeFrom(*columns[col], 0, columns[col]->size());
    }
    accumulated.clear();

    // Fisher-Yates shuffle.
    IColumn::Permutation perm(total_rows);
    std::iota(perm.begin(), perm.end(), 0);
    for (size_t i = total_rows - 1; i > 0; --i)
    {
        const size_t j = rng() % (i + 1);
        std::swap(perm[i], perm[j]);
    }

    Columns result(num_cols);
    for (size_t col = 0; col < num_cols; ++col)
        result[col] = merged[col]->permute(perm, 0);

    return Chunk(std::move(result), total_rows);
}

}
