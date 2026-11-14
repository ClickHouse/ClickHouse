#include <Interpreters/sortBlockByPackedKey.h>

#include <Columns/RadixSortHelper.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/callWithOrderedBits.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Common/RadixSort.h>
#include <Common/iota.h>
#include <base/sort.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <deque>
#include <limits>
#include <type_traits>

namespace DB
{

namespace
{

/// A packed key concatenates the leading integer columns, each reduced by its minimum (or subtracted from its maximum
/// for a descending column), the first one in the most significant bits. Packed keys compare like the rows on these
/// columns, so one stable radix sort of them orders the block by all of them.
struct PackedSortColumn
{
    /// Appends the reduced values of the column to the packed keys: key = (key << bits) | reduced.
    void (*append)(const PackedSortColumn & column, UInt64 * keys, size_t rows) = nullptr;
    /// The same for the rows at the given positions.
    void (*append_at)(const PackedSortColumn & column, UInt64 * keys, const size_t * positions, size_t size) = nullptr;
    const char * data = nullptr;
    /// The minimum of the column for an ascending column, the maximum for a descending one.
    UInt64 base = 0;
    size_t bits = 0;
    /// The width of the column's values.
    size_t value_bits = 0;
    /// Bytes moved per row and pass by the column sort's radix sort of this column, 0 if it compares the values.
    size_t column_sort_work = 0;
};

/// How a sort column can be packed, by its type.
enum class PackedSortType : uint8_t
{
    None,        /// Not packable.
    Range,       /// Packed by the range of its values.
    RangeOrRank, /// A 64-bit integer: packed by the ranks of its values or by their range.
    Rank,        /// A 128-bit integer or a UUID: packed by the ranks of its values.
};

PackedSortType getPackedSortType(const IColumn & column)
{
    if (!column.isFixedAndContiguous())
        return PackedSortType::None;

    const WhichDataType which(column.getDataType());
    if (which.isUInt64() || which.isInt64())
        return PackedSortType::RangeOrRank;
    if (which.isUInt128() || which.isUUID())
        return PackedSortType::Rank;
    if (which.isNativeInteger() || which.isDecimal32() || which.isDecimal64() || which.isDateTime64() || which.isTime64())
        return PackedSortType::Range;
    return PackedSortType::None;
}

template <typename GetBits>
using OrderedBits = std::invoke_result_t<GetBits, const char *>;

template <typename GetBits>
ALWAYS_INLINE OrderedBits<GetBits> valueAt(const char * data, size_t row)
{
    return GetBits{}(data + row * sizeof(OrderedBits<GetBits>));
}

/// Reads a rank of `rankWideColumn`.
struct LoadRank
{
    UInt32 operator()(const char * value) const { return unalignedLoad<UInt32>(value); }
};

/// Calls `f` with the `GetBits` of a column of a packable type; other types never reach it.
template <typename F>
void callWithPackableOrderedBits(const IColumn & column, F && f)
{
    callWithOrderedBits<UInt64>(column, column.sizeOfValueIfFixed(), [&](auto get_bits)
    {
        using GetBits = decltype(get_bits);
        if constexpr (std::is_default_constructible_v<GetBits> && sizeof(OrderedBits<GetBits>) <= sizeof(UInt128))
            f(get_bits);
        else
            chassert(false, "Unexpected column type for a packed sort key");
    });
}

/// The column sort radix-sorts (value, row number) pairs of a numeric column, one pass per byte of the value.
template <typename T>
constexpr size_t columnRadixSortWork()
{
    return sizeof(T) * sizeof(ValueWithIndex<T>);
}

/// `column.bits` is below 64, see the single-column check in `trySortByPackedKey`.
template <typename GetBits, bool descending>
void appendToPackedKey(const PackedSortColumn & column, UInt64 * __restrict keys, size_t rows)
{
    using Bits = OrderedBits<GetBits>;
    const char * __restrict data = column.data;
    const Bits base = static_cast<Bits>(column.base);
    const size_t bits = column.bits;
    chassert(bits < 64);

    for (size_t i = 0; i < rows; ++i)
    {
        const Bits value = valueAt<GetBits>(data, i);
        keys[i] = (keys[i] << bits) | static_cast<Bits>(descending ? base - value : value - base);
    }
}

template <typename GetBits, bool descending>
void appendToPackedKeyAt(const PackedSortColumn & column, UInt64 * __restrict keys, const size_t * __restrict positions, size_t size)
{
    using Bits = OrderedBits<GetBits>;
    const char * __restrict data = column.data;
    const Bits base = static_cast<Bits>(column.base);
    const size_t bits = column.bits;
    chassert(bits < 64);

    for (size_t i = 0; i < size; ++i)
    {
        const Bits value = valueAt<GetBits>(data, positions[i]);
        keys[i] = (keys[i] << bits) | static_cast<Bits>(descending ? base - value : value - base);
    }
}

template <typename GetBits>
void initPackedSortColumn(const char * data, size_t rows, bool descending, PackedSortColumn & column)
{
    using Bits = OrderedBits<GetBits>;
    Bits min = valueAt<GetBits>(data, 0);
    Bits max = min;
    for (size_t i = 1; i < rows; ++i)
    {
        const Bits value = valueAt<GetBits>(data, i);
        min = std::min(min, value);
        max = std::max(max, value);
    }

    column.data = data;
    column.base = descending ? max : min;
    column.bits = std::bit_width(static_cast<UInt64>(static_cast<Bits>(max - min)));
    column.value_bits = sizeof(Bits) * 8;
    column.column_sort_work = columnRadixSortWork<Bits>();
    column.append = descending ? &appendToPackedKey<GetBits, true> : &appendToPackedKey<GetBits, false>;
    column.append_at = descending ? &appendToPackedKeyAt<GetBits, true> : &appendToPackedKeyAt<GetBits, false>;
}

/// 1 if the values strictly increase through the block, -1 if they strictly decrease, judged first by 17 rows spread
/// over the block (0 if these do not), then by all rows (2 or -2 if only the spread rows do).
template <typename GetBits>
int strictOrder(const char * data, size_t rows)
{
    using Bits = OrderedBits<GetBits>;
    int direction = 0;
    for (size_t k = 1; k <= 16; ++k)
    {
        const Bits prev = valueAt<GetBits>(data, (k - 1) * (rows - 1) / 16);
        const Bits value = valueAt<GetBits>(data, k * (rows - 1) / 16);
        const int step = prev < value ? 1 : (value < prev ? -1 : 0);
        if (step == 0 || (direction != 0 && step != direction))
            return 0;
        direction = step;
    }

    size_t i = 1;
    if (direction > 0)
        while (i < rows && valueAt<GetBits>(data, i - 1) < valueAt<GetBits>(data, i))
            ++i;
    else
        while (i < rows && valueAt<GetBits>(data, i) < valueAt<GetBits>(data, i - 1))
            ++i;
    return i == rows ? direction : 2 * direction;
}

int strictOrder(const IColumn & column, size_t rows)
{
    const PackedSortType type = getPackedSortType(column);
    if (type != PackedSortType::Range && type != PackedSortType::RangeOrRank)
        return 0;

    const char * data = column.getRawData().data();
    int order = 0;
    callWithPackableOrderedBits(column, [&](auto get_bits)
    {
        using GetBits = decltype(get_bits);
        if constexpr (sizeof(OrderedBits<GetBits>) <= sizeof(UInt64))
            order = strictOrder<GetBits>(data, rows);
    });
    return order;
}

/// Whether the values never decrease through the block, or never increase for a descending column.
bool inSortOrder(const IColumn & column, size_t rows, bool descending)
{
    const char * data = column.getRawData().data();
    bool in_order = false;
    callWithPackableOrderedBits(column, [&](auto get_bits)
    {
        using GetBits = decltype(get_bits);
        size_t i = 1;
        if (descending)
            while (i < rows && !(valueAt<GetBits>(data, i - 1) < valueAt<GetBits>(data, i)))
                ++i;
        else
            while (i < rows && !(valueAt<GetBits>(data, i) < valueAt<GetBits>(data, i - 1)))
                ++i;
        in_order = i == rows;
    });
    return in_order;
}

constexpr size_t max_rank_bits = 16;
constexpr size_t min_rows_per_rank = 16;
/// Each ranked column keeps a `UInt32` per row until the keys are built.
constexpr size_t max_ranked_columns = 2;

template <typename T>
using WideColumnHash = std::conditional_t<sizeof(T) == sizeof(UInt64), HashCRC32<T>, UInt128HashCRC32>;

struct WideColumnSample
{
    /// The values repeat often enough to be ranked.
    bool few_distinct = true;
    /// The width of the range of the sampled values, a lower bound for the whole column.
    size_t range_bits = 0;
};

/// A Weyl sequence, not a fixed stride that could align with a period of the data; distinct while k << rows.
size_t sampledRow(size_t k, size_t rows)
{
    const UInt64 weyl = static_cast<UInt64>(static_cast<UInt128>(k) * 0x9E3779B97F4A7C15ULL);
    return static_cast<size_t>((static_cast<UInt128>(weyl) * rows) >> 64);
}

/// A sample of s rows drawn from D equally frequent values holds about s^2 / 2D repeated values: with
/// s = 8 sqrt(max_distinct), about 32 of them for exactly `max_distinct` values.
template <typename GetBits>
WideColumnSample sampleWideColumn(const char * data, size_t rows, size_t max_distinct)
{
    using Bits = OrderedBits<GetBits>;
    using Map = HashMap<Bits, UInt32, WideColumnHash<Bits>>;

    WideColumnSample result;
    const size_t sample_size = std::clamp<size_t>(8 * static_cast<size_t>(std::sqrt(static_cast<double>(max_distinct))), 256, 2048);
    if (rows < sample_size * 4)
        return result;

    Map sample(sample_size);
    size_t repeated = 0;
    Bits min = valueAt<GetBits>(data, 0);
    Bits max = min;
    for (size_t k = 0; k < sample_size; ++k)
    {
        const Bits value = valueAt<GetBits>(data, sampledRow(k, rows));
        typename Map::LookupResult it = nullptr;
        bool inserted = false;
        sample.emplace(value, it, inserted);
        repeated += !inserted;
        min = std::min(min, value);
        max = std::max(max, value);
    }

    /// Require at least 3/4 of the repeats expected from `max_distinct` values.
    result.few_distinct = repeated * 8 * max_distinct >= 3 * sample_size * sample_size;
    if constexpr (sizeof(Bits) <= sizeof(UInt64))
        result.range_bits = std::bit_width(static_cast<UInt64>(max - min));
    else
        result.range_bits = sizeof(Bits) * 8;
    return result;
}

/// The number of distinct values of the whole column by linear counting: n distinct values set about m (1 - e^(-n/m))
/// of the m bits of a bitmap. Returns `limit + 1` as soon as the bits set show more than `limit` values.
template <typename GetBits>
size_t estimateDistinct(const char * data, size_t rows, size_t limit)
{
    using Bits = OrderedBits<GetBits>;

    /// 2 to 4 bits per value up to the limit.
    const size_t log2_bits = std::max<size_t>(12, static_cast<size_t>(std::bit_width(2 * limit)));
    chassert(log2_bits <= 32);
    const size_t bit_mask = (size_t(1) << log2_bits) - 1;
    const double bits = static_cast<double>(size_t(1) << log2_bits);
    const size_t max_set_bits = static_cast<size_t>(-std::expm1(-static_cast<double>(limit) / bits) * bits);

    PODArray<UInt64> bitmap;
    bitmap.resize_fill((size_t(1) << log2_bits) / 64, 0);
    UInt64 * __restrict words = bitmap.data();

    size_t set_bits = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        /// Multiplicative hashing of the 32-bit hash: the top `log2_bits` bits of the product modulo 2^32.
        const UInt64 product = UInt64{static_cast<UInt32>(WideColumnHash<Bits>{}(valueAt<GetBits>(data, i)))} * 0x9E3779B9ULL;
        const size_t bit = static_cast<size_t>(product >> (32 - log2_bits)) & bit_mask;
        const UInt64 mask = UInt64(1) << (bit % 64);
        set_bits += !(words[bit / 64] & mask);
        words[bit / 64] |= mask;
        if (set_bits > max_set_bits)
            return limit + 1;
    }

    return static_cast<size_t>(-std::log1p(-static_cast<double>(set_bits) / bits) * bits);
}

/// Replaces the values by their ranks among the distinct values of the block, unless there are more than `max_distinct` of them.
template <typename GetBits>
bool rankWideColumn(
    const char * data,
    size_t rows,
    bool descending,
    size_t max_distinct,
    size_t expected_distinct,
    PaddedPODArray<UInt32> & ranks,
    PackedSortColumn & column)
{
    using Bits = OrderedBits<GetBits>;
    using Map = HashMap<Bits, UInt32, WideColumnHash<Bits>>;

    Map ids(expected_distinct);
    std::vector<Bits> distinct;
    distinct.reserve(expected_distinct);
    ranks.resize(rows);

    for (size_t i = 0; i < rows; ++i)
    {
        const Bits value = valueAt<GetBits>(data, i);
        typename Map::LookupResult it = nullptr;
        bool inserted = false;
        ids.emplace(value, it, inserted);
        if (inserted)
        {
            if (distinct.size() == max_distinct)
                return false;
            it->getMapped() = static_cast<UInt32>(distinct.size());
            distinct.push_back(value);
        }
        ranks[i] = it->getMapped();
    }

    const size_t num_distinct = distinct.size();
    std::vector<UInt32> order(num_distinct);
    iota(order.data(), num_distinct, UInt32(0));
    ::sort(order.begin(), order.end(), [&](UInt32 lhs, UInt32 rhs) { return distinct[lhs] < distinct[rhs]; });

    std::vector<UInt32> rank_of_id(num_distinct);
    for (size_t rank = 0; rank < num_distinct; ++rank)
        rank_of_id[order[rank]] = static_cast<UInt32>(descending ? num_distinct - 1 - rank : rank);

    for (size_t i = 0; i < rows; ++i)
        ranks[i] = rank_of_id[ranks[i]];

    column.data = reinterpret_cast<const char *>(ranks.data());
    column.base = 0;
    column.bits = std::bit_width(num_distinct - 1);
    column.value_bits = sizeof(Bits) * 8;
    /// The column sort compares UUIDs and 128-bit integers.
    if constexpr (sizeof(Bits) <= sizeof(UInt64))
        column.column_sort_work = columnRadixSortWork<Bits>();
    else
        column.column_sort_work = 0;
    column.append = &appendToPackedKey<LoadRank, false>;
    column.append_at = &appendToPackedKeyAt<LoadRank, false>;
    return true;
}

enum class WidePacking : uint8_t
{
    NotWide,  /// Not a 64-bit or 128-bit integer column.
    Ranked,   /// Packed by ranks.
    UseRange, /// To be packed by its range, if the whole range fits.
    Stop,     /// Cannot be packed into the bits left.
};

/// A wide column (a hash, an identifier, a UUID) is packed by the ranks of its values if a sample shows they repeat.
template <typename GetBits>
WidePacking packWideColumn(
    const char * data,
    size_t rows,
    bool descending,
    bool is_last,
    size_t bits_left,
    bool may_rank,
    PaddedPODArray<UInt32> & ranks,
    PackedSortColumn & column)
{
    using Bits = OrderedBits<GetBits>;
    const size_t max_distinct = std::min(size_t(1) << std::min(max_rank_bits, bits_left), rows / min_rows_per_rank);
    const WideColumnSample sample = sampleWideColumn<GetBits>(data, rows, max_distinct);
    const bool range_may_fit = sizeof(Bits) <= sizeof(UInt64) && sample.range_bits <= bits_left;
    const bool rank = !range_may_fit || (!is_last && sample.range_bits > max_rank_bits);

    if (may_rank && rank && sample.few_distinct)
    {
        /// A sample does not show a tail of unique values.
        const size_t limit = max_distinct + max_distinct / 16;
        const size_t estimate = estimateDistinct<GetBits>(data, rows, limit);
        if (estimate <= limit
            && rankWideColumn<GetBits>(data, rows, descending, max_distinct, std::min(max_distinct, estimate + estimate / 8 + 16), ranks, column))
            return WidePacking::Ranked;
    }

    /// Packing would stop after a first column this wide, and a single wide column is not worth packing.
    if (bits_left == 64 && sample.range_bits > 64 - 8)
        return WidePacking::Stop;
    return range_may_fit ? WidePacking::UseRange : WidePacking::Stop;
}

/// Packs a column of a packable type, by ranks or by range. Returns false if packing stops at this column.
bool tryPackColumn(
    const IColumn & column,
    PackedSortType type,
    size_t rows,
    bool descending,
    bool is_last,
    size_t bits_left,
    std::deque<PaddedPODArray<UInt32>> & ranks,
    PackedSortColumn & packed,
    bool & ranked)
{
    const char * data = column.getRawData().data();
    const bool may_rank = ranks.size() < max_ranked_columns;
    WidePacking wide = WidePacking::NotWide;
    bool packed_by_range = false;

    callWithPackableOrderedBits(column, [&](auto get_bits)
    {
        using GetBits = decltype(get_bits);
        using Bits = OrderedBits<GetBits>;

        if constexpr (sizeof(Bits) == sizeof(UInt64) || sizeof(Bits) == sizeof(UInt128))
        {
            if (type != PackedSortType::Range)
            {
                wide = packWideColumn<GetBits>(data, rows, descending, is_last, bits_left, may_rank, ranks.emplace_back(), packed);
                if (wide != WidePacking::Ranked)
                    ranks.pop_back();
            }
        }

        if constexpr (sizeof(Bits) <= sizeof(UInt64))
        {
            if (wide == WidePacking::NotWide || wide == WidePacking::UseRange)
            {
                initPackedSortColumn<GetBits>(data, rows, descending, packed);
                packed_by_range = true;
            }
        }
    });

    ranked = wide == WidePacking::Ranked;
    if (ranked)
        return true;
    return packed_by_range && packed.bits <= bits_left;
}

void addEqualRanges(size_t rows, EqualRanges & equal_ranges, auto && key_at)
{
    size_t begin = 0;
    for (size_t i = 1; i < rows; ++i)
    {
        if (key_at(i) != key_at(begin))
        {
            if (i - begin > 1)
                equal_ranges.emplace_back(begin, i);
            begin = i;
        }
    }
    if (rows - begin > 1)
        equal_ranges.emplace_back(begin, rows);
}

/// The work of the radix sort in `sortByPackedKey`, in bytes moved per row and pass: 4 passes for keys of up to
/// 32 bits and 8 for 64 bits, over keys holding the row number or over (key, row number) pairs.
size_t packedSortWork(size_t total_bits, size_t index_bits)
{
    if (total_bits + index_bits <= 32)
        return 4 * sizeof(UInt32);
    if (total_bits <= 32)
        return 4 * sizeof(ValueWithIndex<UInt32>);
    if (total_bits + index_bits <= 64)
        return 8 * sizeof(UInt64);
    return 8 * sizeof(ValueWithIndex<UInt64>);
}

/// Whether the rows of the block are nearly distinct on the packed columns. Among s distinct rows sampled from rows
/// that repeat r times on average, about s^2 (r - 1) / 2n pairs are equal: with s = 2 sqrt(n), fewer than
/// s^2 / 2n = 2 equal pairs mean that most rows are distinct.
bool packedColumnsNearlyUnique(const std::vector<PackedSortColumn> & columns, size_t rows)
{
    const size_t sample_size = std::min<size_t>(2 * static_cast<size_t>(std::sqrt(static_cast<double>(rows))), 16384);
    PaddedPODArray<size_t> positions(sample_size);
    for (size_t k = 0; k < sample_size; ++k)
        positions[k] = sampledRow(k, rows);

    PaddedPODArray<UInt64> keys(sample_size, 0);
    for (const auto & column : columns)
        column.append_at(column, keys.data(), positions.data(), sample_size);

    ::sort(keys.begin(), keys.end());
    size_t equal = 0;
    for (size_t k = 1; k < sample_size; ++k)
        equal += keys[k] == keys[k - 1];
    return equal * 2 * rows < sample_size * sample_size;
}

/// Whether the column sort is all radix sorts (ranges of at least 256 rows on average; shorter ones are sorted by
/// comparisons through the permutation) that move fewer bytes than the radix sort of the packed key.
bool columnSortIsCheaper(const std::vector<PackedSortColumn> & columns, size_t rows, size_t total_bits, size_t index_bits)
{
    size_t work = 0;
    size_t prefix_bits = 0;
    for (const auto & column : columns)
    {
        if (column.column_sort_work == 0 || prefix_bits > 48 || (size_t(256) << prefix_bits) > rows)
            return false;
        work += column.column_sort_work;
        prefix_bits += column.bits;
    }
    return packedSortWork(total_bits, index_bits) > work;
}

/// Sorts keys that hold the row number in their low `index_bits` bits, so they are distinct and their order is
/// the stable order of the packed keys.
template <typename T>
void sortKeysWithRowNumbers(
    T * keys, size_t rows, size_t index_bits, bool try_presorted, bool need_equal_ranges, IColumn::Permutation & permutation, EqualRanges & equal_ranges)
{
    if (!(try_presorted && trySort(keys, keys + rows)))
        RadixSort<RadixSortUIntTraits<T>>::executeLSD(keys, rows);

    const T index_mask = static_cast<T>((T(1) << index_bits) - 1);
    for (size_t i = 0; i < rows; ++i)
        permutation[i] = keys[i] & index_mask;

    if (need_equal_ranges)
        addEqualRanges(rows, equal_ranges, [&](size_t i) { return keys[i] >> index_bits; });
}

/// Sorts (key, row number) pairs by a stable radix sort of the keys.
template <typename Key>
void sortKeysWithIndices(
    PaddedPODArray<UInt64> & keys, size_t rows, bool try_presorted, bool need_equal_ranges, IColumn::Permutation & permutation, EqualRanges & equal_ranges)
{
    PaddedPODArray<ValueWithIndex<Key>> pairs(rows);
    for (size_t i = 0; i < rows; ++i)
        pairs[i] = {static_cast<Key>(keys[i]), static_cast<UInt32>(i)};
    PaddedPODArray<UInt64>().swap(keys); /// Free the memory before sorting.

    /// Only for an unstable sort: `trySort` can reorder equal keys.
    const bool presorted = try_presorted
        && trySort(pairs.begin(), pairs.end(), [](const ValueWithIndex<Key> & lhs, const ValueWithIndex<Key> & rhs) { return lhs.value < rhs.value; });

    if (!presorted && !need_equal_ranges)
    {
        RadixSort<RadixSortTraits<Key>>::executeLSD(pairs.data(), rows, /*reverse=*/ false, permutation.data());
        return;
    }

    if (!presorted)
        RadixSort<RadixSortTraits<Key>>::executeLSD(pairs.data(), rows);
    for (size_t i = 0; i < rows; ++i)
        permutation[i] = pairs[i].index;
    if (need_equal_ranges)
        addEqualRanges(rows, equal_ranges, [&](size_t i) { return pairs[i].value; });
}

void sortByPackedKey(
    std::vector<PackedSortColumn> & packed_columns,
    std::deque<PaddedPODArray<UInt32>> & ranks,
    size_t total_bits,
    size_t rows,
    bool stable,
    bool need_equal_ranges,
    IColumn::Permutation & permutation,
    EqualRanges & equal_ranges)
{
    PaddedPODArray<UInt64> keys(rows, 0);
    for (const auto & column : packed_columns)
        column.append(column, keys.data(), rows);
    ranks.clear();

    /// Distinct keys already in their order or in the reverse one need no sort and leave no equal ranges.
    if (std::adjacent_find(keys.begin(), keys.end(), std::greater_equal<>()) == keys.end())
    {
        iota(permutation.data(), rows, IColumn::Permutation::value_type(0));
        return;
    }
    if (std::adjacent_find(keys.begin(), keys.end(), std::less_equal<>()) == keys.end())
    {
        for (size_t i = 0; i < rows; ++i)
            permutation[i] = rows - 1 - i;
        return;
    }

    if (std::is_sorted(keys.begin(), keys.end()))
    {
        iota(permutation.data(), rows, IColumn::Permutation::value_type(0));
        if (need_equal_ranges)
            addEqualRanges(rows, equal_ranges, [&](size_t i) { return keys[i]; });
        return;
    }

    /// Runs of equal keys are taken from the last one, each in its own order, as a stable sort requires.
    if (std::is_sorted(keys.begin(), keys.end(), std::greater<>()))
    {
        size_t position = 0;
        for (size_t end = rows; end > 0;)
        {
            size_t begin = end - 1;
            while (begin > 0 && keys[begin - 1] == keys[end - 1])
                --begin;
            if (need_equal_ranges && end - begin > 1)
                equal_ranges.emplace_back(position, position + (end - begin));
            for (size_t i = begin; i < end; ++i)
                permutation[position++] = i;
            end = begin;
        }
        return;
    }

    const bool try_presorted = !stable;
    const size_t index_bits = std::bit_width(rows - 1);
    if (total_bits + index_bits <= 32)
    {
        PaddedPODArray<UInt32> narrow_keys(rows);
        for (size_t i = 0; i < rows; ++i)
            narrow_keys[i] = static_cast<UInt32>((keys[i] << index_bits) | i);
        PaddedPODArray<UInt64>().swap(keys); /// Free the memory before sorting.
        sortKeysWithRowNumbers(narrow_keys.data(), rows, index_bits, try_presorted, need_equal_ranges, permutation, equal_ranges);
    }
    else if (total_bits <= 32)
    {
        sortKeysWithIndices<UInt32>(keys, rows, try_presorted, need_equal_ranges, permutation, equal_ranges);
    }
    else if (total_bits + index_bits <= 64)
    {
        for (size_t i = 0; i < rows; ++i)
            keys[i] = (keys[i] << index_bits) | i;
        sortKeysWithRowNumbers(keys.data(), rows, index_bits, try_presorted, need_equal_ranges, permutation, equal_ranges);
    }
    else
    {
        sortKeysWithIndices<UInt64>(keys, rows, try_presorted, need_equal_ranges, permutation, equal_ranges);
    }
}

}

size_t trySortByPackedKey(
    const ColumnsWithSortDescriptions & columns,
    size_t rows,
    bool stable,
    bool need_equal_ranges,
    IColumn::Permutation & permutation,
    EqualRanges & equal_ranges)
{
    /// The same lower threshold as for the radix sort of a single column.
    if (rows < 256 || rows > std::numeric_limits<UInt32>::max())
        return 0;

    size_t first_non_const = columns.size();
    size_t last_non_const = 0;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!columns[i].column_const)
        {
            first_non_const = std::min(first_non_const, i);
            last_non_const = i;
        }
    }

    /// A strictly monotone first column orders the block alone; an unstable sort leaves an almost sorted one to the
    /// column sort, which handles it in about linear time.
    if (first_non_const < columns.size() && columns[first_non_const].description.collator == nullptr)
    {
        const int order = strictOrder(*columns[first_non_const].column, rows);
        if (order == 1 || order == -1)
        {
            if (order == columns[first_non_const].description.direction)
                iota(permutation.data(), rows, IColumn::Permutation::value_type(0));
            else
                for (size_t i = 0; i < rows; ++i)
                    permutation[i] = rows - 1 - i;
            return columns.size();
        }
        if (order != 0 && !stable)
            return 0;
    }

    std::vector<PackedSortColumn> packed_columns;
    std::deque<PaddedPODArray<UInt32>> ranks;
    size_t sorted_entries = 0;
    size_t total_bits = 0;
    const size_t index_bits = std::bit_width(rows - 1);
    bool last_column_in_order = false;

    /// Kept while the packed columns do not change.
    size_t columns_not_unique = 0;
    auto nearly_unique = [&]
    {
        if (total_bits == 0 || rows < 1024 || (size_t(2) << total_bits) < rows || packed_columns.size() == columns_not_unique)
            return false;
        if (packedColumnsNearlyUnique(packed_columns, rows))
            return true;
        columns_not_unique = packed_columns.size();
        return false;
    };

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column = columns[i];
        if (column.column_const)
        {
            ++sorted_entries;
            continue;
        }

        if (column.description.collator != nullptr)
            break;

        const PackedSortType type = getPackedSortType(*column.column);
        if (type == PackedSortType::None)
            break;

        const bool descending = column.description.direction == -1;

        /// A last column already in order stays in order under a stable sort by the columns before it.
        if (i == last_non_const && i != first_non_const && !need_equal_ranges && inSortOrder(*column.column, rows, descending))
        {
            last_column_in_order = true;
            sorted_entries = columns.size();
            break;
        }

        const size_t bits_left = 64 - total_bits;
        PackedSortColumn packed;
        bool ranked = false;
        if (!tryPackColumn(*column.column, type, rows, descending, i == last_non_const, bits_left, ranks, packed, ranked))
            break;

        /// After columns that nearly tell the rows apart, a column is not worth more radix passes or wider elements.
        if (packedSortWork(total_bits + packed.bits, index_bits) > packedSortWork(total_bits, index_bits) && nearly_unique())
        {
            if (ranked)
                ranks.pop_back();
            break;
        }

        const bool first_bits = total_bits == 0;
        total_bits += packed.bits;
        ++sorted_entries;
        packed_columns.push_back(packed);

        /// A following non-constant column would hardly fit.
        if (total_bits > 64 - 8)
            break;

        /// A first column too wide to be packed alone is better sorted by itself if it nearly tells the rows apart.
        if (first_bits && i < last_non_const && packed.bits > packed.value_bits / 2 && nearly_unique())
            break;
    }

    if (packed_columns.empty() || columnSortIsCheaper(packed_columns, rows, total_bits, index_bits))
        return 0;

    /// The following columns are sorted within the ranges of rows equal on the packed ones.
    need_equal_ranges = need_equal_ranges || sorted_entries < columns.size();

    /// A column that is constant in the block does not contribute to the key.
    std::erase_if(packed_columns, [](const PackedSortColumn & column) { return column.bits == 0; });

    if (total_bits == 0)
    {
        iota(permutation.data(), rows, IColumn::Permutation::value_type(0));
        if (need_equal_ranges)
            equal_ranges.emplace_back(0, rows);
        return sorted_entries;
    }

    /// A single column is worth packing only if its key is much narrower than its values. Every packed column is
    /// therefore narrower than 64 bits, so that appending it never shifts the key by 64.
    if (packed_columns.size() == 1 && packed_columns[0].bits > packed_columns[0].value_bits / 2)
        return 0;

    sortByPackedKey(packed_columns, ranks, total_bits, rows, stable || last_column_in_order, need_equal_ranges, permutation, equal_ranges);
    return sorted_entries;
}

}
