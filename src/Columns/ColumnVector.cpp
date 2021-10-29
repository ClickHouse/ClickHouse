#include "ColumnVector.h"

#include <pdqsort.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/MaskOperations.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/NaNUtils.h>
#include <Common/RadixSort.h>
#include <Common/SipHash.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <base/sort.h>
#include <base/unaligned.h>
#include <base/bit_cast.h>
#include <base/scope_guard.h>

#include <cmath>
#include <cstring>

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

template <typename T>
StringRef ColumnVector<T>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    auto * pos = arena.allocContinue(sizeof(T), begin);
    unalignedStore<T>(pos, data[n]);
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char * ColumnVector<T>::deserializeAndInsertFromArena(const char * pos)
{
    data.emplace_back(unalignedLoad<T>(pos));
    return pos + sizeof(T);
}

template <typename T>
const char * ColumnVector<T>::skipSerializedInArena(const char * pos) const
{
    return pos + sizeof(T);
}

template <typename T>
void ColumnVector<T>::updateHashWithValue(size_t n, SipHash & hash) const
{
    hash.update(data[n]);
}

template <typename T>
void ColumnVector<T>::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = data.size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const T * begin = data.data();
    const T * end = begin + s;
    UInt32 * hash_data = hash.getData().data();

    while (begin < end)
    {
        *hash_data = intHashCRC32(*begin, *hash_data);
        ++begin;
        ++hash_data;
    }
}

template <typename T>
void ColumnVector<T>::updateHashFast(SipHash & hash) const
{
    hash.update(reinterpret_cast<const char *>(data.data()), size() * sizeof(data[0]));
}

template <typename T>
struct ColumnVector<T>::less
{
    const Self & parent;
    int nan_direction_hint;
    less(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

template <typename T>
struct ColumnVector<T>::greater
{
    const Self & parent;
    int nan_direction_hint;
    greater(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

template <typename T>
struct ColumnVector<T>::equals
{
    const Self & parent;
    int nan_direction_hint;
    equals(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::equals(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};


namespace
{
    template <typename T>
    struct ValueWithIndex
    {
        T value;
        UInt32 index;
    };

    template <typename T>
    struct RadixSortTraits : RadixSortNumTraits<T>
    {
        using Element = ValueWithIndex<T>;
        using Result = size_t;

        static T & extractKey(Element & elem) { return elem.value; }
        static size_t extractResult(Element & elem) { return elem.index; }
    };
}


template <typename T>
void ColumnVector<T>::getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    size_t s = data.size();
    res.resize(s);

    if (s == 0)
        return;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        for (size_t i = 0; i < s; ++i)
            res[i] = i;

        if (reverse)
            partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this, nan_direction_hint));
        else
            partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this, nan_direction_hint));
    }
    else
    {
        /// A case for radix sort
        if constexpr (is_arithmetic_v<T> && !is_big_int_v<T>)
        {
            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (s >= 256 && s <= std::numeric_limits<UInt32>::max())
            {
                PaddedPODArray<ValueWithIndex<T>> pairs(s);
                for (UInt32 i = 0; i < UInt32(s); ++i)
                    pairs[i] = {data[i], i};

                RadixSort<RadixSortTraits<T>>::executeLSD(pairs.data(), s, reverse, res.data());

                /// Radix sort treats all NaNs to be greater than all numbers.
                /// If the user needs the opposite, we must move them accordingly.
                if (std::is_floating_point_v<T> && nan_direction_hint < 0)
                {
                    size_t nans_to_move = 0;

                    for (size_t i = 0; i < s; ++i)
                    {
                        if (isNaN(data[res[reverse ? i : s - 1 - i]]))
                            ++nans_to_move;
                        else
                            break;
                    }

                    if (nans_to_move)
                    {
                        std::rotate(std::begin(res), std::begin(res) + (reverse ? nans_to_move : s - nans_to_move), std::end(res));
                    }
                }
                return;
            }
        }

        /// Default sorting algorithm.
        for (size_t i = 0; i < s; ++i)
            res[i] = i;

        if (reverse)
            pdqsort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else
            pdqsort(res.begin(), res.end(), less(*this, nan_direction_hint));
    }
}

template <typename T>
void ColumnVector<T>::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_range) const
{
    auto sort = [](auto begin, auto end, auto pred) { pdqsort(begin, end, pred); };
    auto partial_sort = [](auto begin, auto mid, auto end, auto pred) { ::partial_sort(begin, mid, end, pred); };

    if (reverse)
        this->updatePermutationImpl(
            limit, res, equal_range,
            greater(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            sort, partial_sort);
    else
        this->updatePermutationImpl(
            limit, res, equal_range,
            less(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            sort, partial_sort);
}

template <typename T>
MutableColumnPtr ColumnVector<T>::cloneResized(size_t size) const
{
    auto res = this->create();

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count)
            memset(static_cast<void *>(&new_col.data[count]), 0, (size - count) * sizeof(ValueType));
    }

    return res;
}

template <typename T>
UInt64 ColumnVector<T>::get64(size_t n [[maybe_unused]]) const
{
    if constexpr (is_arithmetic_v<T>)
        return bit_cast<UInt64>(data[n]);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as UInt64", TypeName<T>);
}

template <typename T>
inline Float64 ColumnVector<T>::getFloat64(size_t n [[maybe_unused]]) const
{
    if constexpr (is_arithmetic_v<T>)
        return static_cast<Float64>(data[n]);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as Float64", TypeName<T>);
}

template <typename T>
Float32 ColumnVector<T>::getFloat32(size_t n [[maybe_unused]]) const
{
    if constexpr (is_arithmetic_v<T>)
        return static_cast<Float32>(data[n]);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as Float32", TypeName<T>);
}

template <typename T>
void ColumnVector<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnVector & src_vec = assert_cast<const ColumnVector &>(src);

    if (start + length > src_vec.data.size())
        throw Exception("Parameters start = "
            + toString(start) + ", length = "
            + toString(length) + " are out of bound in ColumnVector<T>::insertRangeFrom method"
            " (data.size() = " + toString(src_vec.data.size()) + ").",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
ColumnPtr ColumnVector<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create();
    Container & res_data = res->getData();

    if (result_size_hint)
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

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
        UInt64 mask = Bytes64MaskToBits64Mask(filt_pos);

        if (0xffffffffffffffff == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            while (mask)
            {
                size_t index = __builtin_ctzll(mask);
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

template <typename T>
void ColumnVector<T>::expand(const IColumn::Filter & mask, bool inverted)
{
    expandDataByMask<T>(data, mask, inverted);
}

template <typename T>
void ColumnVector<T>::applyZeroMap(const IColumn::Filter & filt, bool inverted)
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + size;
    T * data_pos = data.data();

    if (inverted)
    {
        for (; filt_pos < filt_end; ++filt_pos, ++data_pos)
            if (!*filt_pos)
                *data_pos = 0;
    }
    else
    {
        for (; filt_pos < filt_end; ++filt_pos, ++data_pos)
            if (*filt_pos)
                *data_pos = 0;
    }
}

template <typename T>
ColumnPtr ColumnVector<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
}

template <typename T>
ColumnPtr ColumnVector<T>::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename T>
ColumnPtr ColumnVector<T>::replicate(const IColumn::Offsets & offsets) const
{
    const size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == size)
        return this->create();

    auto res = this->create(offsets.back());

    auto it = res->getData().begin(); // NOLINT
    for (size_t i = 0; i < size; ++i)
    {
        const auto span_end = res->getData().begin() + offsets[i]; // NOLINT
        for (; it != span_end; ++it)
            *it = data[i];
    }

    return res;
}

template <typename T>
void ColumnVector<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
void ColumnVector<T>::getExtremes(Field & min, Field & max) const
{
    size_t size = data.size();

    if (size == 0)
    {
        min = T(0);
        max = T(0);
        return;
    }

    bool has_value = false;

    /** Skip all NaNs in extremes calculation.
        * If all values are NaNs, then return NaN.
        * NOTE: There exist many different NaNs.
        * Different NaN could be returned: not bit-exact value as one of NaNs from column.
        */

    T cur_min = NaNOrZero<T>();
    T cur_max = NaNOrZero<T>();

    for (const T & x : data)
    {
        if (isNaN(x))
            continue;

        if (!has_value)
        {
            cur_min = x;
            cur_max = x;
            has_value = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = NearestFieldType<T>(cur_min);
    max = NearestFieldType<T>(cur_max);
}


#pragma GCC diagnostic ignored "-Wold-style-cast"

template <typename T>
ColumnPtr ColumnVector<T>::compress() const
{
    size_t source_size = data.size() * sizeof(T);

    /// Don't compress small blocks.
    if (source_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto compressed = ColumnCompressed::compressBuffer(data.data(), source_size, false);

    if (!compressed)
        return ColumnCompressed::wrap(this->getPtr());

    return ColumnCompressed::create(data.size(), compressed->size(),
        [compressed = std::move(compressed), column_size = data.size()]
        {
            auto res = ColumnVector<T>::create(column_size);
            ColumnCompressed::decompressBuffer(
                compressed->data(), res->getData().data(), compressed->size(), column_size * sizeof(T));
            return res;
        });
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<UInt8>;
template class ColumnVector<UInt16>;
template class ColumnVector<UInt32>;
template class ColumnVector<UInt64>;
template class ColumnVector<UInt128>;
template class ColumnVector<UInt256>;
template class ColumnVector<Int8>;
template class ColumnVector<Int16>;
template class ColumnVector<Int32>;
template class ColumnVector<Int64>;
template class ColumnVector<Int128>;
template class ColumnVector<Int256>;
template class ColumnVector<Float32>;
template class ColumnVector<Float64>;
template class ColumnVector<UUID>;

}
