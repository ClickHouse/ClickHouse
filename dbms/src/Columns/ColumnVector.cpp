#include "ColumnVector.h"

#include <cstring>
#include <cmath>
#include <common/unaligned.h>
#include <Common/Exception.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/NaNUtils.h>
#include <Common/RadixSort.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsCommon.h>
#include <DataStreams/ColumnGathererStream.h>
#include <ext/bit_cast.h>
#include <pdqsort.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


template <typename T>
StringRef ColumnVector<T>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    auto pos = arena.allocContinue(sizeof(T), begin);
    unalignedStore(pos, data[n]);
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char * ColumnVector<T>::deserializeAndInsertFromArena(const char * pos)
{
    data.push_back(unalignedLoad<T>(pos));
    return pos + sizeof(T);
}

template <typename T>
void ColumnVector<T>::updateHashWithValue(size_t n, SipHash & hash) const
{
    hash.update(data[n]);
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
        static T & extractKey(Element & elem) { return elem.value; }
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
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this, nan_direction_hint));
    }
    else
    {
        /// A case for radix sort
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, UInt128>)
        {
            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (s >= 256 && s <= std::numeric_limits<UInt32>::max())
            {
                PaddedPODArray<ValueWithIndex<T>> pairs(s);
                for (UInt32 i = 0; i < s; ++i)
                    pairs[i] = {data[i], i};

                RadixSort<RadixSortTraits<T>>::executeLSD(pairs.data(), s);

                /// Radix sort treats all NaNs to be greater than all numbers.
                /// If the user needs the opposite, we must move them accordingly.
                size_t nans_to_move = 0;
                if (std::is_floating_point_v<T> && nan_direction_hint < 0)
                {
                    for (ssize_t i = s - 1; i >= 0; --i)
                    {
                        if (isNaN(pairs[i].value))
                            ++nans_to_move;
                        else
                            break;
                    }
                }

                if (reverse)
                {
                    if (nans_to_move)
                    {
                        for (size_t i = 0; i < s - nans_to_move; ++i)
                            res[i] = pairs[s - nans_to_move - 1 - i].index;
                        for (size_t i = s - nans_to_move; i < s; ++i)
                            res[i] = pairs[s - 1 - (i - (s - nans_to_move))].index;
                    }
                    else
                    {
                        for (size_t i = 0; i < s; ++i)
                            res[s - 1 - i] = pairs[i].index;
                    }
                }
                else
                {
                    if (nans_to_move)
                    {
                        for (size_t i = 0; i < nans_to_move; ++i)
                            res[i] = pairs[i + s - nans_to_move].index;
                        for (size_t i = nans_to_move; i < s; ++i)
                            res[i] = pairs[i - nans_to_move].index;
                    }
                    else
                    {
                        for (size_t i = 0; i < s; ++i)
                            res[i] = pairs[i].index;
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
const char * ColumnVector<T>::getFamilyName() const
{
    return TypeName<T>::get();
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
            memset(static_cast<void *>(&new_col.data[count]), static_cast<int>(value_type()), (size - count) * sizeof(value_type));
    }

    return res;
}

template <typename T>
UInt64 ColumnVector<T>::get64(size_t n) const
{
    return ext::bit_cast<UInt64>(data[n]);
}

template <typename T>
void ColumnVector<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnVector & src_vec = static_cast<const ColumnVector &>(src);

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

#ifdef __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_sse)
    {
        int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

        if (0 == mask)
        {
            /// Nothing is inserted.
        }
        else if (0xFFFF == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            for (size_t i = 0; i < SIMD_BYTES; ++i)
                if (filt_pos[i])
                    res_data.push_back(data_pos[i]);
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
#endif

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
ColumnPtr ColumnVector<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(limit);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
ColumnPtr ColumnVector<T>::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename T>
ColumnPtr ColumnVector<T>::replicate(const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == size)
        return this->create();

    auto res = this->create();
    typename Self::Container & res_data = res->getData();
    res_data.reserve(offsets.back());

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

    for (const T x : data)
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

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<UInt8>;
template class ColumnVector<UInt16>;
template class ColumnVector<UInt32>;
template class ColumnVector<UInt64>;
template class ColumnVector<UInt128>;
template class ColumnVector<Int8>;
template class ColumnVector<Int16>;
template class ColumnVector<Int32>;
template class ColumnVector<Int64>;
template class ColumnVector<Int128>;
template class ColumnVector<Float32>;
template class ColumnVector<Float64>;
}
