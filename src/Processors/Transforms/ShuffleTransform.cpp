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
#include <cmath>
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

/// The maximum number of buckets to scatter the rows into.
/// Every bucket has at least about `max_block_size` rows, so a bucket fits in the CPU cache in most cases,
/// and the number of small pieces created while scattering is bounded.
constexpr size_t max_num_buckets = 256;

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

/// A uniformly random permutation of [0, n): the Fisher-Yates shuffle.
IColumn::Permutation randomPermutation(pcg64 & rng, size_t n)
{
    IColumn::Permutation permutation(n);
    std::iota(permutation.begin(), permutation.end(), 0);
    for (size_t i = n; i > 1; --i)
        std::swap(permutation[i - 1], permutation[randomBelow(rng, i)]);
    return permutation;
}

}

PartialShuffleTransform::PartialShuffleTransform(
    SharedHeader header,
    size_t key_position_,
    int direction_,
    UInt64 limit_,
    size_t max_block_size_,
    size_t range_index_,
    size_t num_ranges_)
    : IAccumulatingTransform(header, header)
    , key_position(key_position_)
    , direction(direction_)
    , limit(limit_)
    , max_block_size(std::max<size_t>(max_block_size_, 1))
    , range_index(range_index_)
    , num_ranges(std::max<size_t>(num_ranges_, 1))
    , is_key_uint32(isKeyUInt32(*header, key_position_))
    , rng(randomSeed())
    , threshold(std::numeric_limits<UInt64>::max())
{
    empty_columns.resize(header->columns());
    for (size_t i = 0; i < header->columns(); ++i)
    {
        const auto & column = header->getByPosition(i).column;
        if (i != key_position && !isColumnConst(*column))
            empty_columns[i] = removeSpecialRepresentations(column)->cloneEmpty();
    }

    if (limit && num_ranges != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shuffle with limit cannot use ranges of keys");

    if (limit)
    {
        accumulated_columns.resize(empty_columns.size());
        for (size_t i = 0; i < empty_columns.size(); ++i)
            if (empty_columns[i])
                accumulated_columns[i] = empty_columns[i]->cloneEmpty();
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
    {
        if (empty_columns[i])
            columns[i] = columns[i]->convertToFullColumnIfConst();
        else
            columns[i] = nullptr;
    }

    if (limit)
    {
        consumeWithLimit(columns, num_rows);
        return;
    }

    chunks.push_back(std::move(columns));
    accumulated_rows += num_rows;
}

UInt64 PartialShuffleTransform::sampleRowsToSkip()
{
    /// The number of rows before the next row with a key less than the threshold has the geometric distribution
    /// with the probability p = threshold / 2^64 of success.
    const double p = static_cast<double>(threshold) / 18446744073709551616.0;
    if (p >= 1.0)
        return 0;
    if (p <= 0.0)
        return std::numeric_limits<UInt64>::max();

    /// A uniformly distributed number in (0, 1].
    const double u = (static_cast<double>(rng() >> 11) + 1.0) / 9007199254740992.0;
    const double skip = std::floor(std::log(u) / std::log1p(-p));
    if (!(skip < 1.8e19))
        return std::numeric_limits<UInt64>::max();
    return static_cast<UInt64>(skip);
}

void PartialShuffleTransform::consumeWithLimit(const Columns & columns, size_t num_rows)
{
    /// Before the first pruning, the threshold is the maximum value and all rows are accepted.
    if (threshold == std::numeric_limits<UInt64>::max())
    {
        for (size_t i = 0; i < columns.size(); ++i)
            if (accumulated_columns[i])
                accumulated_columns[i]->insertRangeFrom(*columns[i], 0, num_rows);

        reservoir_keys.reserve(reservoir_keys.size() + num_rows);
        for (size_t row = 0; row < num_rows; ++row)
            reservoir_keys.push_back(rng());

        accumulated_rows += num_rows;
    }
    else
    {
        /// Only a row with a key less than the threshold can be selected. Instead of generating a key for every row,
        /// skip the rows with larger keys: the number of skipped rows is sampled from the geometric distribution,
        /// and the key of a selected row is uniformly distributed in [0, threshold).
        /// The expected number of selected rows of a stream of n rows is O(limit * log(n / limit)), so this is much
        /// faster than looking at every row. The pruning lowers the threshold; the number of rows to skip is then
        /// sampled again, which is correct because the geometric distribution is memoryless.
        auto indices = ColumnUInt64::create();
        auto & indices_data = indices->getData();

        UInt64 row = rows_to_skip;
        while (row < num_rows)
        {
            indices_data.push_back(row);
            reservoir_keys.push_back(randomBelow(rng, threshold));

            const UInt64 skip = sampleRowsToSkip();
            row = (skip >= std::numeric_limits<UInt64>::max() - row) ? std::numeric_limits<UInt64>::max() : row + 1 + skip;
        }
        rows_to_skip = (row == std::numeric_limits<UInt64>::max()) ? row : row - num_rows;

        const size_t num_accepted = indices_data.size();
        if (num_accepted == 0)
            return;

        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (accumulated_columns[i])
            {
                auto selected = columns[i]->index(*indices, 0);
                accumulated_columns[i]->insertRangeFrom(*selected, 0, num_accepted);
            }
        }

        accumulated_rows += num_accepted;
    }

    /// Prune when there are `limit` excess rows (but not too often if the limit is small),
    /// so that the amortized cost of pruning is O(1) per accepted row.
    if (accumulated_rows > limit && accumulated_rows - limit >= std::max<UInt64>(limit, max_block_size))
    {
        pruneReservoir();
        rows_to_skip = sampleRowsToSkip();
    }
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
        prepareResultWithLimit();
    else
        scatterIntoBuckets();
}

void PartialShuffleTransform::prepareResultWithLimit()
{
    pruneReservoir();

    /// The keys of the selected rows matter (they are compared with the keys of other streams when merging),
    /// so the rows are ordered by their keys. The number of rows is at most `limit`.
    IColumn::Permutation permutation(accumulated_rows);
    std::iota(permutation.begin(), permutation.end(), 0);
    ::sort(permutation.begin(), permutation.end(),
        [&](size_t lhs, size_t rhs) { return reservoir_keys[lhs] < reservoir_keys[rhs]; });

    part_keys.resize(accumulated_rows);
    for (size_t i = 0; i < accumulated_rows; ++i)
        part_keys[i] = reservoir_keys[permutation[i]];
    Keys().swap(reservoir_keys);

    part_columns.resize(accumulated_columns.size());
    for (size_t i = 0; i < accumulated_columns.size(); ++i)
        if (accumulated_columns[i])
            part_columns[i] = accumulated_columns[i]->permute(permutation, 0);
    accumulated_columns.clear();
}

void PartialShuffleTransform::scatterIntoBuckets()
{
    if (accumulated_rows == 0)
        return;

    const size_t num_buckets = std::clamp<size_t>(accumulated_rows / max_block_size, 1, max_num_buckets);

    buckets.resize(num_buckets);
    for (auto & bucket : buckets)
    {
        bucket.resize(empty_columns.size());
        for (size_t i = 0; i < empty_columns.size(); ++i)
            if (empty_columns[i])
                bucket[i] = empty_columns[i]->cloneEmpty();
    }
    bucket_rows.assign(num_buckets, 0);

    IColumn::Selector selector;
    for (auto & columns : chunks)
    {
        size_t num_rows = 0;
        for (const auto & column : columns)
            if (column)
                num_rows = column->size();

        if (num_buckets == 1)
        {
            for (size_t i = 0; i < columns.size(); ++i)
                if (columns[i])
                    buckets[0][i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
            columns.clear();
            continue;
        }

        if (num_rows == 0)
        {
            /// There are no accumulated columns, only the number of rows matters.
            columns.clear();
            continue;
        }

        /// Every row goes into a random bucket.
        selector.resize(num_rows);
        for (auto & bucket : selector)
        {
            bucket = randomBelow(rng, num_buckets);
            ++bucket_rows[bucket];
        }

        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (!columns[i])
                continue;

            auto parts = columns[i]->scatter(num_buckets, selector);
            for (size_t bucket = 0; bucket < num_buckets; ++bucket)
                buckets[bucket][i]->insertRangeFrom(*parts[bucket], 0, parts[bucket]->size());
        }

        /// Release the memory of the chunk.
        columns.clear();
    }
    chunks.clear();

    if (num_buckets == 1)
    {
        bucket_rows[0] = accumulated_rows;
    }
    else if (std::all_of(empty_columns.begin(), empty_columns.end(), [](const auto & column) { return !column; }))
    {
        /// No accumulated columns: distribute the number of rows among the buckets.
        for (size_t row = 0; row < accumulated_rows; ++row)
            ++bucket_rows[randomBelow(rng, num_buckets)];
    }
}

bool PartialShuffleTransform::prepareNextBucket()
{
    const size_t num_buckets = buckets.size();
    while (next_bucket < num_buckets && bucket_rows[next_bucket] == 0)
        ++next_bucket;

    if (next_bucket >= num_buckets)
        return false;

    const size_t bucket = next_bucket;
    ++next_bucket;

    const size_t num_rows = bucket_rows[bucket];
    const auto permutation = randomPermutation(rng, num_rows);

    part_columns.assign(empty_columns.size(), nullptr);
    for (size_t i = 0; i < empty_columns.size(); ++i)
        if (buckets[bucket][i])
            part_columns[i] = buckets[bucket][i]->permute(permutation, 0);
    MutableColumns().swap(buckets[bucket]);

    /// The range and the bucket number are the high part of the key: the keys of the bucket are in [begin, end).
    part_keys.resize(num_rows);
    const unsigned __int128 total_buckets = static_cast<unsigned __int128>(num_ranges) * num_buckets;
    if (total_buckets == 1)
    {
        for (auto & key : part_keys)
            key = rng();
    }
    else
    {
        const unsigned __int128 global_bucket = static_cast<unsigned __int128>(range_index) * num_buckets + bucket;
        const UInt64 begin = static_cast<UInt64>((global_bucket << 64) / total_buckets);
        const UInt64 end = static_cast<UInt64>(((global_bucket + 1) << 64) / total_buckets);
        const UInt64 width = end - begin; /// `end` is zero (2^64 wraps) for the last bucket, and the subtraction is still correct.
        for (auto & key : part_keys)
            key = begin + randomBelow(rng, width);
    }
    RadixSort<RadixSortUIntTraits<UInt64>>::executeLSD(part_keys.data(), part_keys.size());

    part_offset = 0;
    return true;
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
            T value = static_cast<T>(part_keys[offset + i] >> shift);
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

    if (part_offset >= part_keys.size())
    {
        if (limit || !prepareNextBucket())
        {
            part_columns.clear();
            Keys().swap(part_keys);
            return {};
        }
    }

    const size_t num_rows = part_keys.size();
    const size_t length = std::min(max_block_size, num_rows - part_offset);
    const bool whole_part = part_offset == 0 && length == num_rows;

    const auto & header = getOutputPort().getHeader();
    Columns columns(header.columns());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (i == key_position)
            columns[i] = makeKeyColumn(part_offset, length);
        else if (part_columns[i])
            columns[i] = whole_part ? std::move(part_columns[i]) : part_columns[i]->cut(part_offset, length);
        else
            columns[i] = header.getByPosition(i).column->cloneResized(length);
    }

    part_offset += length;
    return Chunk(std::move(columns), length);
}

}
