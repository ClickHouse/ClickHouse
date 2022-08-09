#include "ColumnVector.h"

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
#include <Common/TargetSpecific.h>
#include <Common/assert_cast.h>
#include <base/sort.h>
#include <base/unaligned.h>
#include <base/bit_cast.h>
#include <base/scope_guard.h>

#include <bit>
#include <cmath>
#include <cstring>

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>
#include <llvm/IR/IRBuilder.h>
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
struct ColumnVector<T>::less_stable
{
    const Self & parent;
    int nan_direction_hint;
    less_stable(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        if (unlikely(parent.data[lhs] == parent.data[rhs]))
            return lhs < rhs;

        if constexpr (std::is_floating_point_v<T>)
        {
            if (unlikely(std::isnan(parent.data[lhs]) && std::isnan(parent.data[rhs])))
            {
                return lhs < rhs;
            }
        }

        return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
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
struct ColumnVector<T>::greater_stable
{
    const Self & parent;
    int nan_direction_hint;
    greater_stable(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        if (unlikely(parent.data[lhs] == parent.data[rhs]))
            return lhs < rhs;

        if constexpr (std::is_floating_point_v<T>)
        {
            if (unlikely(std::isnan(parent.data[lhs]) && std::isnan(parent.data[rhs])))
            {
                return lhs < rhs;
            }
        }

        return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
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

#if USE_EMBEDDED_COMPILER

template <typename T>
bool ColumnVector<T>::isComparatorCompilable() const
{
    /// TODO: for std::is_floating_point_v<T> we need implement is_nan in LLVM IR.
    return std::is_integral_v<T>;
}

template <typename T>
llvm::Value * ColumnVector<T>::compileComparator(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs, llvm::Value *) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    if constexpr (std::is_integral_v<T>)
    {
        // a > b ? 1 : (a < b ? -1 : 0);

        bool is_signed = std::is_signed_v<T>;

        auto * lhs_greater_than_rhs_result = llvm::ConstantInt::getSigned(b.getInt8Ty(), 1);
        auto * lhs_less_than_rhs_result = llvm::ConstantInt::getSigned(b.getInt8Ty(), -1);
        auto * lhs_equals_rhs_result = llvm::ConstantInt::getSigned(b.getInt8Ty(), 0);

        auto * lhs_greater_than_rhs = is_signed ? b.CreateICmpSGT(lhs, rhs) : b.CreateICmpUGT(lhs, rhs);
        auto * lhs_less_than_rhs = is_signed ? b.CreateICmpSLT(lhs, rhs) : b.CreateICmpULT(lhs, rhs);
        auto * if_lhs_less_than_rhs_result = b.CreateSelect(lhs_less_than_rhs, lhs_less_than_rhs_result, lhs_equals_rhs_result);

        return b.CreateSelect(lhs_greater_than_rhs, lhs_greater_than_rhs_result, if_lhs_less_than_rhs_result);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method compileComparator is not supported for type {}", TypeName<T>);
    }
}

#endif

template <typename T>
void ColumnVector<T>::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
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

        if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
            ::partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
            ::partial_sort(res.begin(), res.begin() + limit, res.end(), less_stable(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
            ::partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
            ::partial_sort(res.begin(), res.begin() + limit, res.end(), greater_stable(*this, nan_direction_hint));
    }
    else
    {
        /// A case for radix sort
        /// LSD RadixSort is stable
        if constexpr (is_arithmetic_v<T> && !is_big_int_v<T>)
        {
            bool reverse = direction == IColumn::PermutationSortDirection::Descending;
            bool ascending = direction == IColumn::PermutationSortDirection::Ascending;
            bool sort_is_stable = stability == IColumn::PermutationSortStability::Stable;

            /// TODO: LSD RadixSort is currently not stable if direction is descending, or value is floating point
            bool use_radix_sort = (sort_is_stable && ascending && !std::is_floating_point_v<T>) || !sort_is_stable;

            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (s >= 256 && s <= std::numeric_limits<UInt32>::max() && use_radix_sort)
            {
                PaddedPODArray<ValueWithIndex<T>> pairs(s);
                for (UInt32 i = 0; i < static_cast<UInt32>(s); ++i)
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

        if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
            ::sort(res.begin(), res.end(), less(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
            ::sort(res.begin(), res.end(), less_stable(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
            ::sort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
            ::sort(res.begin(), res.end(), greater_stable(*this, nan_direction_hint));
    }
}

template <typename T>
void ColumnVector<T>::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    bool reverse = direction == IColumn::PermutationSortDirection::Descending;
    bool ascending = direction == IColumn::PermutationSortDirection::Ascending;
    bool sort_is_stable = stability == IColumn::PermutationSortStability::Stable;

    auto sort = [&](auto begin, auto end, auto pred)
    {
        /// A case for radix sort
        if constexpr (is_arithmetic_v<T> && !is_big_int_v<T>)
        {
            /// TODO: LSD RadixSort is currently not stable if direction is descending, or value is floating point
            bool use_radix_sort = (sort_is_stable && ascending && !std::is_floating_point_v<T>) || !sort_is_stable;
            size_t size = end - begin;

            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (size >= 256 && size <= std::numeric_limits<UInt32>::max() && use_radix_sort)
            {
                PaddedPODArray<ValueWithIndex<T>> pairs(size);
                size_t index = 0;

                for (auto * it = begin; it != end; ++it)
                {
                    pairs[index] = {data[*it], static_cast<UInt32>(*it)};
                    ++index;
                }

                RadixSort<RadixSortTraits<T>>::executeLSD(pairs.data(), size, reverse, begin);

                /// Radix sort treats all NaNs to be greater than all numbers.
                /// If the user needs the opposite, we must move them accordingly.
                if (std::is_floating_point_v<T> && nan_direction_hint < 0)
                {
                    size_t nans_to_move = 0;

                    for (size_t i = 0; i < size; ++i)
                    {
                        if (isNaN(data[begin[reverse ? i : size - 1 - i]]))
                            ++nans_to_move;
                        else
                            break;
                    }

                    if (nans_to_move)
                    {
                        std::rotate(begin, begin + (reverse ? nans_to_move : size - nans_to_move), end);
                    }
                }

                return;
            }
        }

        ::sort(begin, end, pred);
    };
    auto partial_sort = [](auto begin, auto mid, auto end, auto pred) { ::partial_sort(begin, mid, end, pred); };

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            less(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            sort, partial_sort);
    }
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            less_stable(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            sort, partial_sort);
    }
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            greater(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            sort, partial_sort);
    }
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            greater_stable(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            sort, partial_sort);
    }
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

static inline UInt64 blsr(UInt64 mask)
{
#ifdef __BMI__
    return _blsr_u64(mask);
#else
    return mask & (mask-1);
#endif
}

DECLARE_DEFAULT_CODE(
template <typename T, typename Container, size_t SIMD_BYTES>
inline void doFilterAligned(const UInt8 *& filt_pos, const UInt8 *& filt_end_aligned, const T *& data_pos, Container & res_data)
{
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
                mask = blsr(mask);
            }
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
}
)

namespace
{
template <typename T, typename Container>
void reserve(Container & res_data, size_t reserve_size)
{
#if defined(MEMORY_SANITIZER)
    res_data.resize_fill(reserve_size, static_cast<T>(0)); // MSan doesn't recognize that all allocated memory is written by AVX-512 intrinsics.
#else
    res_data.resize(reserve_size);
#endif
}
}

DECLARE_AVX512VBMI2_SPECIFIC_CODE(
template <size_t ELEMENT_WIDTH>
inline void compressStoreAVX512(const void *src, void *dst, const UInt64 mask)
{
    __m512i vsrc = _mm512_loadu_si512(src);
    if constexpr (ELEMENT_WIDTH == 1)
        _mm512_mask_compressstoreu_epi8(dst, static_cast<__mmask64>(mask), vsrc);
    else if constexpr (ELEMENT_WIDTH == 2)
        _mm512_mask_compressstoreu_epi16(dst, static_cast<__mmask32>(mask), vsrc);
    else if constexpr (ELEMENT_WIDTH == 4)
        _mm512_mask_compressstoreu_epi32(dst, static_cast<__mmask16>(mask), vsrc);
    else if constexpr (ELEMENT_WIDTH == 8)
        _mm512_mask_compressstoreu_epi64(dst, static_cast<__mmask8>(mask), vsrc);
}

template <typename T, typename Container, size_t SIMD_BYTES>
inline void doFilterAligned(const UInt8 *& filt_pos, const UInt8 *& filt_end_aligned, const T *& data_pos, Container & res_data)
{
    static constexpr size_t VEC_LEN = 64;   /// AVX512 vector length - 64 bytes
    static constexpr size_t ELEMENT_WIDTH = sizeof(T);
    static constexpr size_t ELEMENTS_PER_VEC = VEC_LEN / ELEMENT_WIDTH;
    static constexpr UInt64 KMASK = 0xffffffffffffffff >> (64 - ELEMENTS_PER_VEC);

    size_t current_offset = res_data.size();
    size_t reserve_size = res_data.size();
    size_t alloc_size = SIMD_BYTES * 2;

    while (filt_pos < filt_end_aligned)
    {
        /// to avoid calling resize too frequently, resize to reserve buffer.
        if (reserve_size - current_offset < SIMD_BYTES)
        {
            reserve_size += alloc_size;
            reserve<T>(res_data, reserve_size);
            alloc_size *= 2;
        }

        UInt64 mask = bytes64MaskToBits64Mask(filt_pos);

        if (0xffffffffffffffff == mask)
        {
            for (size_t i = 0; i < SIMD_BYTES; i += ELEMENTS_PER_VEC)
                _mm512_storeu_si512(reinterpret_cast<void *>(&res_data[current_offset + i]),
                        _mm512_loadu_si512(reinterpret_cast<const void *>(data_pos + i)));
            current_offset += SIMD_BYTES;
        }
        else
        {
            if (mask)
            {
                for (size_t i = 0; i < SIMD_BYTES; i += ELEMENTS_PER_VEC)
                {
                    compressStoreAVX512<ELEMENT_WIDTH>(reinterpret_cast<const void *>(data_pos + i),
                            reinterpret_cast<void *>(&res_data[current_offset]), mask & KMASK);
                    current_offset += std::popcount(mask & KMASK);
                    /// prepare mask for next iter, if ELEMENTS_PER_VEC = 64, no next iter
                    if (ELEMENTS_PER_VEC < 64)
                    {
                        mask >>= ELEMENTS_PER_VEC;
                    }
                }
            }
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
    /// resize to the real size.
    res_data.resize(current_offset);
}
)

template <typename T>
ColumnPtr ColumnVector<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size);

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

#if USE_MULTITARGET_CODE
    static constexpr bool VBMI2_CAPABLE = sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8;
    if (VBMI2_CAPABLE && isArchSupported(TargetArch::AVX512VBMI2))
        TargetSpecific::AVX512VBMI2::doFilterAligned<T, Container, SIMD_BYTES>(filt_pos, filt_end_aligned, data_pos, res_data);
    else
#endif
        TargetSpecific::Default::doFilterAligned<T, Container, SIMD_BYTES>(filt_pos, filt_end_aligned, data_pos, res_data);

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
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size);

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

#ifdef __SSE2__

namespace
{
    /** Optimization for ColumnVector replicate using SIMD instructions.
      * For such optimization it is important that data is right padded with 15 bytes.
      *
      * Replicate span size is offsets[i] - offsets[i - 1].
      *
      * Split spans into 3 categories.
      * 1. Span with 0 size. Continue iteration.
      *
      * 2. Span with 1 size. Update pointer from which data must be copied into result.
      * Then if we see span with size 1 or greater than 1 copy data directly into result data and reset pointer.
      * Example:
      * Data: 1 2 3 4
      * Offsets: 1 2 3 4
      * Result data: 1 2 3 4
      *
      * 3. Span with size greater than 1. Save single data element into register and copy it into result data.
      * Example:
      * Data: 1 2 3 4
      * Offsets: 4 4 4 4
      * Result data: 1 1 1 1
      *
      * Additional handling for tail is needed if pointer from which data must be copied from span with size 1 is not null.
      */
    template<typename IntType>
    requires (std::is_same_v<IntType, Int32> || std::is_same_v<IntType, UInt32>)
    void replicateSSE42Int32(const IntType * __restrict data, IntType * __restrict result_data, const IColumn::Offsets & offsets)
    {
        const IntType * data_copy_begin_ptr = nullptr;
        size_t offsets_size = offsets.size();

        for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
        {
            size_t span = offsets[offset_index] - offsets[offset_index - 1];
            if (span == 1)
            {
                if (!data_copy_begin_ptr)
                    data_copy_begin_ptr = data + offset_index;

                continue;
            }

            /// Copy data

            if (data_copy_begin_ptr)
            {
                size_t copy_size = (data + offset_index) - data_copy_begin_ptr;
                bool remainder = copy_size % 4;
                size_t sse_copy_counter = (copy_size / 4) + remainder;
                auto * result_data_copy = result_data;

                while (sse_copy_counter)
                {
                    __m128i copy_batch = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data_copy_begin_ptr));
                    _mm_storeu_si128(reinterpret_cast<__m128i *>(result_data_copy), copy_batch);
                    result_data_copy += 4;
                    data_copy_begin_ptr += 4;
                    --sse_copy_counter;
                }

                result_data += copy_size;
                data_copy_begin_ptr = nullptr;
            }

            if (span == 0)
                continue;

            /// Copy single data element into result data

            bool span_remainder = span % 4;
            size_t copy_counter = (span / 4) + span_remainder;
            auto * result_data_tmp = result_data;
            __m128i copy_element_data = _mm_set1_epi32(data[offset_index]);

            while (copy_counter)
            {
                _mm_storeu_si128(reinterpret_cast<__m128i *>(result_data_tmp), copy_element_data);
                result_data_tmp += 4;
                --copy_counter;
            }

            result_data += span;
        }

        /// Copy tail if needed

        if (data_copy_begin_ptr)
        {
            size_t copy_size = (data + offsets_size) - data_copy_begin_ptr;
            bool remainder = copy_size % 4;
            size_t sse_copy_counter = (copy_size / 4) + remainder;

            while (sse_copy_counter)
            {
                __m128i copy_batch = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data_copy_begin_ptr));
                _mm_storeu_si128(reinterpret_cast<__m128i *>(result_data), copy_batch);
                result_data += 4;
                data_copy_begin_ptr += 4;
                --sse_copy_counter;
            }
        }
    }
}

#endif

template <typename T>
ColumnPtr ColumnVector<T>::replicate(const IColumn::Offsets & offsets) const
{
    const size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == size)
        return this->create();

    auto res = this->create(offsets.back());

#ifdef __SSE2__
    if constexpr (std::is_same_v<T, UInt32>)
    {
        replicateSSE42Int32(getData().data(), res->getData().data(), offsets);
        return res;
    }
#endif

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
        [compressed = std::move(compressed), column_size = data_size]
        {
            auto res = ColumnVector<T>::create(column_size);
            ColumnCompressed::decompressBuffer(
                compressed->data(), res->getData().data(), compressed->size(), column_size * sizeof(T));
            return res;
        });
}

template <typename T>
ColumnPtr ColumnVector<T>::createWithOffsets(const IColumn::Offsets & offsets, const Field & default_field, size_t total_rows, size_t shift) const
{
    if (offsets.size() + shift != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Incompatible sizes of offsets ({}), shift ({}) and size of column {}", offsets.size(), shift, size());

    auto res = this->create();
    auto & res_data = res->getData();

    T default_value = safeGet<T>(default_field);
    res_data.resize_fill(total_rows, default_value);
    for (size_t i = 0; i < offsets.size(); ++i)
        res_data[offsets[i]] = data[i + shift];

    return res;
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
