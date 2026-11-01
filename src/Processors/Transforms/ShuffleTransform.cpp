#include <Processors/Transforms/ShuffleTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/RadixSort.h>
#include <Common/randomSeed.h>
#include <base/sort.h>

#include <algorithm>
#include <limits>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

bool isKeyUInt32(const Block & header, size_t key_position)
{
    if (key_position >= header.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shuffle key position {} is out of range for header {}", key_position, header.dumpStructure());

    const auto & key_type = header.getByPosition(key_position).type;
    WhichDataType which(key_type);
    if (which.isUInt64())
        return false;
    if (which.isUInt32())
        return true;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Shuffle key column must be UInt64 or UInt32, got {}", key_type->getName());
}

/// A uniformly distributed random number in [0, n), see https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
/// The bias is at most n / 2^64, which is negligible.
inline UInt64 randomBelow(pcg64 & rng, UInt64 n)
{
    return static_cast<UInt64>((static_cast<unsigned __int128>(rng()) * n) >> 64);
}

}

PartialShuffleTransform::PartialShuffleTransform(
    SharedHeader header,
    size_t key_position_,
    int direction_,
    UInt64 limit_,
    size_t max_block_size_)
    : IAccumulatingTransform(header, header)
    , key_position(key_position_)
    , direction(direction_)
    , limit(limit_)
    , max_block_size(std::max<size_t>(max_block_size_, 1))
    , is_key_uint32(isKeyUInt32(*header, key_position_))
    , rng(randomSeed())
    , threshold(std::numeric_limits<UInt64>::max())
{
    accumulated_columns.resize(header->columns());
    for (size_t i = 0; i < header->columns(); ++i)
    {
        const auto & column = header->getByPosition(i).column;
        if (i != key_position && !isColumnConst(*column))
            accumulated_columns[i] = removeSpecialRepresentations(column)->cloneEmpty();
    }
}

void PartialShuffleTransform::consume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    removeSpecialColumnRepresentations(chunk);
    Columns columns = chunk.detachColumns();

    for (size_t i = 0; i < columns.size(); ++i)
        if (accumulated_columns[i])
            columns[i] = columns[i]->convertToFullColumnIfConst();

    if (limit)
    {
        consumeWithLimit(columns, num_rows);
        return;
    }

    for (size_t i = 0; i < columns.size(); ++i)
        if (accumulated_columns[i])
            accumulated_columns[i]->insertRangeFrom(*columns[i], 0, num_rows);

    accumulated_rows += num_rows;
}

void PartialShuffleTransform::consumeWithLimit(const Columns & columns, size_t num_rows)
{
    Keys keys(num_rows);
    for (auto & key : keys)
        key = rng();

    /// Before the first pruning, the threshold is the maximum value and all rows are accepted.
    size_t num_accepted = num_rows;
    IColumn::Filter filter;
    if (threshold != std::numeric_limits<UInt64>::max())
    {
        filter.resize(num_rows);
        num_accepted = 0;
        for (size_t row = 0; row < num_rows; ++row)
        {
            filter[row] = keys[row] < threshold;
            num_accepted += filter[row];
        }
    }

    if (num_accepted == 0)
        return;

    if (num_accepted == num_rows)
    {
        for (size_t i = 0; i < columns.size(); ++i)
            if (accumulated_columns[i])
                accumulated_columns[i]->insertRangeFrom(*columns[i], 0, num_rows);

        reservoir_keys.insert(keys.begin(), keys.end());
    }
    else
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (accumulated_columns[i])
            {
                auto filtered = columns[i]->filter(filter, num_accepted);
                accumulated_columns[i]->insertRangeFrom(*filtered, 0, num_accepted);
            }
        }

        for (size_t row = 0; row < num_rows; ++row)
            if (filter[row])
                reservoir_keys.push_back(keys[row]);
    }

    accumulated_rows += num_accepted;

    /// Prune when there are `limit` excess rows (but not too often if the limit is small),
    /// so that the amortized cost of pruning is O(1) per accepted row.
    if (accumulated_rows > limit && accumulated_rows - limit >= std::max<UInt64>(limit, max_block_size))
        pruneReservoir();
}

void PartialShuffleTransform::pruneReservoir()
{
    if (accumulated_rows <= limit)
        return;

    /// Find the rows with the `limit` smallest keys.
    IColumn::Permutation indices(accumulated_rows);
    std::iota(indices.begin(), indices.end(), 0);
    std::nth_element(indices.begin(), indices.begin() + (limit - 1), indices.end(),
        [&](size_t lhs, size_t rhs) { return reservoir_keys[lhs] < reservoir_keys[rhs]; });

    threshold = reservoir_keys[indices[limit - 1]];

    IColumn::Filter filter(accumulated_rows, 0);
    for (size_t i = 0; i < limit; ++i)
        filter[indices[i]] = 1;

    for (auto & column : accumulated_columns)
        if (column)
            column = IColumn::mutate(column->filter(filter, limit));

    Keys kept_keys;
    kept_keys.reserve(limit);
    for (size_t row = 0; row < accumulated_rows; ++row)
        if (filter[row])
            kept_keys.push_back(reservoir_keys[row]);

    reservoir_keys.swap(kept_keys);
    accumulated_rows = limit;
}

void PartialShuffleTransform::prepareResult()
{
    result_prepared = true;

    if (limit)
        pruneReservoir();

    const size_t num_rows = accumulated_rows;

    result_columns.resize(accumulated_columns.size());
    for (size_t i = 0; i < accumulated_columns.size(); ++i)
        if (accumulated_columns[i])
            result_columns[i] = std::move(accumulated_columns[i]);
    accumulated_columns.clear();

    result_permutation.resize(num_rows);
    std::iota(result_permutation.begin(), result_permutation.end(), 0);

    if (limit)
    {
        /// The keys of the selected rows matter (they are compared with the keys of other streams when merging),
        /// so the rows are ordered by their keys. The number of rows is at most `limit`.
        ::sort(result_permutation.begin(), result_permutation.end(),
            [&](size_t lhs, size_t rhs) { return reservoir_keys[lhs] < reservoir_keys[rhs]; });

        result_keys.resize(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
            result_keys[i] = reservoir_keys[result_permutation[i]];

        Keys().swap(reservoir_keys);
        return;
    }

    /// The Fisher-Yates shuffle: a uniformly random permutation in O(n).
    for (size_t i = num_rows; i > 1; --i)
        std::swap(result_permutation[i - 1], result_permutation[randomBelow(rng, i)]);

    /// Independent random keys in sorted order: radix sort is O(n).
    result_keys.resize(num_rows);
    for (auto & key : result_keys)
        key = rng();
    RadixSort<RadixSortUIntTraits<UInt64>>::executeLSD(result_keys.data(), result_keys.size());
}

ColumnPtr PartialShuffleTransform::makeKeyColumn(size_t offset, size_t length) const
{
    /// The internal keys are 64-bit. For a `UInt32` key, take the high bits, which is also uniformly distributed
    /// and keeps the order. For descending order, invert the bits, which keeps the distribution and reverses the order.
    auto make = [&]<typename T>()
    {
        auto column = ColumnVector<T>::create(length);
        auto & data = column->getData();
        constexpr size_t shift = (sizeof(UInt64) - sizeof(T)) * 8;
        for (size_t i = 0; i < length; ++i)
        {
            T value = static_cast<T>(result_keys[offset + i] >> shift);
            data[i] = direction < 0 ? static_cast<T>(~value) : value;
        }
        return column;
    };

    if (is_key_uint32)
        return make.template operator()<UInt32>();
    return make.template operator()<UInt64>();
}

Chunk PartialShuffleTransform::generate()
{
    if (!result_prepared)
        prepareResult();

    const size_t num_rows = result_permutation.size();
    if (output_offset >= num_rows)
        return {};

    const size_t length = std::min(max_block_size, num_rows - output_offset);

    auto indexes = ColumnUInt64::create(length);
    auto & indexes_data = indexes->getData();
    for (size_t i = 0; i < length; ++i)
        indexes_data[i] = result_permutation[output_offset + i];

    const auto & header = getOutputPort().getHeader();
    Columns columns(header.columns());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (i == key_position)
            columns[i] = makeKeyColumn(output_offset, length);
        else if (result_columns[i])
            columns[i] = result_columns[i]->index(*indexes, 0);
        else
            columns[i] = header.getByPosition(i).column->cloneResized(length);
    }

    output_offset += length;

    /// Release the memory as soon as possible.
    if (output_offset == num_rows)
    {
        result_columns.clear();
        IColumn::Permutation().swap(result_permutation);
        Keys().swap(result_keys);
    }

    return Chunk(std::move(columns), length);
}

}
