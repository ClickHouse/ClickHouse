#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/Hash.h>
#include <Common/RadixSort.h>
#include <Common/SipHash.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/iota.h>

#include <base/sort.h>

#include <IO/WriteHelpers.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/MaskOperations.h>
#include <Columns/RadixSortHelper.h>
#include <Processors/Transforms/ColumnGathererTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

template <is_decimal T>
#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnDecimal<T>::compareAt(size_t n, size_t m, const IColumn & rhs_, int) const
#else
int ColumnDecimal<T>::doCompareAt(size_t n, size_t m, const IColumn & rhs_, int) const
#endif
{
    auto & other = static_cast<const Self &>(rhs_);
    const T & a = data[n];
    const T & b = other.data[m];

    if (scale == other.scale)
        return a > b ? 1 : (a < b ? -1 : 0);
    return decimalLess<T>(b, a, other.scale, scale) ? 1 : (decimalLess<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <is_decimal T>
const char * ColumnDecimal<T>::deserializeAndInsertFromArena(const char * pos)
{
    data.push_back(unalignedLoad<T>(pos));
    return pos + sizeof(T);
}

template <is_decimal T>
const char * ColumnDecimal<T>::skipSerializedInArena(const char * pos) const
{
    return pos + sizeof(T);
}

template <is_decimal T>
UInt64 ColumnDecimal<T>::get64([[maybe_unused]] size_t n) const
{
    if constexpr (sizeof(T) > sizeof(UInt64))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method get64 is not supported for {}", getFamilyName());
    else
        return static_cast<NativeT>(data[n]);
}

template <is_decimal T>
void ColumnDecimal<T>::updateHashWithValue(size_t n, SipHash & hash) const
{
    hash.update(data[n].value);
}

template <is_decimal T>
WeakHash32 ColumnDecimal<T>::getWeakHash32() const
{
    auto s = data.size();
    WeakHash32 hash(s);

    const T * begin = data.data();
    const T * end = begin + s;
    UInt32 * hash_data = hash.getData().data();

    while (begin < end)
    {
        *hash_data = static_cast<UInt32>(intHashCRC32(*begin, *hash_data));
        ++begin;
        ++hash_data;
    }

    return hash;
}

template <is_decimal T>
void ColumnDecimal<T>::updateHashFast(SipHash & hash) const
{
    hash.update(reinterpret_cast<const char *>(data.data()), size() * sizeof(data[0]));
}

template <is_decimal T>
void ColumnDecimal<T>::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int, IColumn::Permutation & res) const
{
    auto comparator_ascending = [this](size_t lhs, size_t rhs) { return data[lhs] < data[rhs]; };
    auto comparator_ascending_stable = [this](size_t lhs, size_t rhs)
    {
        if (unlikely(data[lhs] == data[rhs]))
            return lhs < rhs;

        return data[lhs] < data[rhs];
    };
    auto comparator_descending = [this](size_t lhs, size_t rhs) { return data[lhs] > data[rhs]; };
    auto comparator_descending_stable = [this](size_t lhs, size_t rhs)
    {
        if (unlikely(data[lhs] == data[rhs]))
            return lhs < rhs;

        return data[lhs] > data[rhs];
    };

    size_t data_size = data.size();
    res.resize_exact(data_size);

    if (limit >= data_size)
        limit = 0;

    iota(res.data(), data_size, IColumn::Permutation::value_type(0));

    if constexpr (is_arithmetic_v<NativeT> && !is_big_int_v<NativeT>)
    {
        if (!limit)
        {
            /// A case for radix sort
            /// LSD RadixSort is stable

            bool reverse = direction == IColumn::PermutationSortDirection::Descending;
            bool ascending = direction == IColumn::PermutationSortDirection::Ascending;
            bool sort_is_stable = stability == IColumn::PermutationSortStability::Stable;

            /// TODO: LSD RadixSort is currently not stable if direction is descending
            bool use_radix_sort = (sort_is_stable && ascending) || !sort_is_stable;

            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (data_size >= 256 && data_size <= std::numeric_limits<UInt32>::max() && use_radix_sort)
            {
                iota(res.data(), data_size, IColumn::Permutation::value_type(0));

                bool try_sort = false;

                if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
                    try_sort = trySort(res.begin(), res.end(), comparator_ascending);
                else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
                    try_sort = trySort(res.begin(), res.end(), comparator_ascending_stable);
                else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
                    try_sort = trySort(res.begin(), res.end(), comparator_descending);
                else
                    try_sort = trySort(res.begin(), res.end(), comparator_descending_stable);

                if (try_sort)
                    return;

                PaddedPODArray<ValueWithIndex<NativeT>> pairs(data_size);
                for (UInt32 i = 0; i < static_cast<UInt32>(data_size); ++i)
                    pairs[i] = {data[i].value, i};

                RadixSort<RadixSortTraits<NativeT>>::executeLSD(pairs.data(), data_size, reverse, res.data());
                return;
            }
        }
    }

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        this->getPermutationImpl(limit, res, comparator_ascending, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        this->getPermutationImpl(limit, res, comparator_ascending_stable, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        this->getPermutationImpl(limit, res, comparator_descending, DefaultSort(), DefaultPartialSort());
    else
        this->getPermutationImpl(limit, res, comparator_descending_stable, DefaultSort(), DefaultPartialSort());
}

template <is_decimal T>
void ColumnDecimal<T>::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                        size_t limit, int, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_descending = [this](size_t lhs, size_t rhs) { return data[lhs] > data[rhs]; };
    auto comparator_descending_stable = [this](size_t lhs, size_t rhs)
    {
        if (unlikely(data[lhs] == data[rhs]))
            return lhs < rhs;

        return data[lhs] > data[rhs];
    };

    auto comparator_ascending = [this](size_t lhs, size_t rhs) { return data[lhs] < data[rhs]; };
    auto comparator_ascending_stable = [this](size_t lhs, size_t rhs)
    {
        if (unlikely(data[lhs] == data[rhs]))
            return lhs < rhs;

        return data[lhs] < data[rhs];
    };
    auto equals_comparator = [this](size_t lhs, size_t rhs) { return data[lhs] == data[rhs]; };
    auto sort = [&](auto begin, auto end, auto pred)
    {
        bool reverse = direction == IColumn::PermutationSortDirection::Descending;
        bool ascending = direction == IColumn::PermutationSortDirection::Ascending;
        bool sort_is_stable = stability == IColumn::PermutationSortStability::Stable;

        /// TODO: LSD RadixSort is currently not stable if direction is descending
        bool use_radix_sort = (sort_is_stable && ascending) || !sort_is_stable;
        size_t size = end - begin;

        if (size >= 256 && size <= std::numeric_limits<UInt32>::max() && use_radix_sort)
        {
            bool try_sort = trySort(begin, end, pred);
            if (try_sort)
                return;

            PaddedPODArray<ValueWithIndex<NativeT>> pairs(size);
            size_t index = 0;

            for (auto * it = begin; it != end; ++it)
            {
                pairs[index] = {data[*it].value, static_cast<UInt32>(*it)};
                ++index;
            }

            RadixSort<RadixSortTraits<NativeT>>::executeLSD(pairs.data(), size, reverse, begin);
            return;
        }

        ::sort(begin, end, pred);
    };
    auto partial_sort = [](auto begin, auto mid, auto end, auto pred) { ::partial_sort(begin, mid, end, pred); };

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            comparator_ascending,
            equals_comparator, sort, partial_sort);
    }
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            comparator_ascending_stable,
            equals_comparator, sort, partial_sort);
    }
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            comparator_descending,
            equals_comparator, sort, partial_sort);
    }
    else
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            comparator_descending_stable,
            equals_comparator, sort, partial_sort);
    }
}

template <is_decimal T>
size_t ColumnDecimal<T>::estimateCardinalityInPermutedRange(const IColumn::Permutation & permutation, const EqualRange & equal_range) const
{
    const size_t range_size = equal_range.size();
    if (range_size <= 1)
        return range_size;

    /// TODO use sampling if the range is too large (e.g. 16k elements, but configurable)
    HashSet<T> elements;
    for (size_t i = equal_range.from; i < equal_range.to; ++i)
    {
        size_t permuted_i = permutation[i];
        elements.insert(data[permuted_i]);
    }
    return elements.size();
}

template <is_decimal T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
}

template <is_decimal T>
MutableColumnPtr ColumnDecimal<T>::cloneResized(size_t size) const
{
    auto res = this->create(0, scale);

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize_exact(size);

        size_t count = std::min(this->size(), size);

        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count)
        {
            void * tail = &new_col.data[count];
            memset(tail, 0, (size - count) * sizeof(T));
        }
    }

    return res;
}

template <is_decimal T>
bool ColumnDecimal<T>::tryInsert(const Field & x)
{
    DecimalField<T> value;
    if (!x.tryGet<DecimalField<T>>(value))
        return false;
    data.push_back(value);
    return true;
}

template <is_decimal T>
void ColumnDecimal<T>::insertData(const char * src, size_t /*length*/)
{
    T tmp;
    memcpy(&tmp, src, sizeof(T));
    data.emplace_back(tmp);
}

template <is_decimal T>
#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDecimal<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnDecimal<T>::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    const ColumnDecimal & src_vec = assert_cast<const ColumnDecimal &>(src);

    if (start + length > src_vec.data.size())
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameters start = {}, length = {} are out of bound "
                        "in ColumnDecimal<T>::insertRangeFrom method (data.size() = {}).",
                        toString(start), toString(length), toString(src_vec.data.size()));

    size_t old_size = data.size();
    data.resize(old_size + length);

    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <is_decimal T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size);

    auto res = this->create(0, scale);
    Container & res_data = res->getData();

    if (result_size_hint)
        res_data.reserve_exact(result_size_hint > 0 ? result_size_hint : size);

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = data.data();

    /** A slightly more optimized version.
    * Based on the assumption that often pieces of consecutive values
    *  completely pass or do not pass the filter.
    * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
    */
    static constexpr size_t SIMD_BYTES = 64;
    const UInt8 * filt_end_aligned = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_aligned)
    {
        UInt64 mask = bytes64MaskToBits64Mask(filt_pos);

        if (0xffffffffffffffff == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            while (mask)
            {
                size_t index = std::countr_zero(mask);
                res_data.push_back(data_pos[index]);
            #ifdef __BMI__
                mask = _blsr_u64(mask);
            #else
                mask = mask & (mask-1);
            #endif
            }
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <is_decimal T>
void ColumnDecimal<T>::expand(const IColumn::Filter & mask, bool inverted)
{
    expandDataByMask<T>(data, mask, inverted);
}

template <is_decimal T>
ColumnPtr ColumnDecimal<T>::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <is_decimal T>
ColumnPtr ColumnDecimal<T>::replicate(const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column.");

    auto res = this->create(0, scale);
    if (0 == size)
        return res;

    typename Self::Container & res_data = res->getData();
    res_data.reserve_exact(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_data.push_back(data[i]);
    }

    return res;
}

template <is_decimal T>
ColumnPtr ColumnDecimal<T>::compress() const
{
    const size_t data_size = data.size();
    const size_t source_size = data_size * sizeof(T);

    /// Don't compress small blocks.
    if (source_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto compressed = ColumnCompressed::compressBuffer(data.data(), source_size, false);

    if (!compressed)
        return ColumnCompressed::wrap(this->getPtr());

    const size_t compressed_size = compressed->size();
    return ColumnCompressed::create(data_size, compressed_size,
        [my_compressed = std::move(compressed), column_size = data_size, my_scale = this->scale]
        {
            auto res = ColumnDecimal<T>::create(column_size, my_scale);
            ColumnCompressed::decompressBuffer(
                my_compressed->data(), res->getData().data(), my_compressed->size(), column_size * sizeof(T));
            return res;
        });
}

template <is_decimal T>
void ColumnDecimal<T>::getExtremes(Field & min, Field & max) const
{
    if (data.empty())
    {
        min = NearestFieldType<T>(T(0), scale);
        max = NearestFieldType<T>(T(0), scale);
        return;
    }

    T cur_min = data[0];
    T cur_max = data[0];

    for (const T & x : data)
    {
        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = NearestFieldType<T>(cur_min, scale);
    max = NearestFieldType<T>(cur_max, scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
template class ColumnDecimal<Decimal256>;
template class ColumnDecimal<DateTime64>;

}
