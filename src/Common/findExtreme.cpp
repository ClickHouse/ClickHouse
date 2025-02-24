#include <DataTypes/IDataType.h>
#include <Common/TargetSpecific.h>
#include <Common/findExtreme.h>

#include <limits>
#include <type_traits>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#include <iostream>
#endif


namespace DB
{

template <has_find_extreme_implementation T>
struct MinComparator
{
    static ALWAYS_INLINE inline const T & cmp(const T & a, const T & b) { return std::min(a, b); }
};

template <has_find_extreme_implementation T>
struct MaxComparator
{
    static ALWAYS_INLINE inline const T & cmp(const T & a, const T & b) { return std::max(a, b); }
};

MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(
        template <has_find_extreme_implementation T, typename ComparatorClass, bool add_all_elements, bool add_if_cond_zero>
        static std::optional<T> NO_INLINE),
    findExtremeImpl,
    MULTITARGET_FUNCTION_BODY((const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t row_begin, size_t row_end) /// NOLINT
    {
        size_t count = row_end - row_begin;
        ptr += row_begin;
        if constexpr (!add_all_elements)
            condition_map += row_begin;

        T ret{};
        size_t i = 0;
        for (; i < count; i++)
        {
            if (add_all_elements || !condition_map[i] == add_if_cond_zero)
            {
                ret = ptr[i];
                break;
            }
        }
        if (i >= count)
            return std::nullopt;

        /// Unroll the loop manually for floating point, since the compiler doesn't do it without fastmath
        /// as it might change the return value
        if constexpr (is_floating_point<T>)
        {
            constexpr size_t unroll_block = 512 / sizeof(T); /// Chosen via benchmarks with AVX2 so YMMV
            size_t unrolled_end = i + (((count - i) / unroll_block) * unroll_block);

            if (i < unrolled_end)
            {
                T partial_min[unroll_block];
                for (size_t unroll_it = 0; unroll_it < unroll_block; unroll_it++)
                    partial_min[unroll_it] = ret;

                while (i < unrolled_end)
                {
                    for (size_t unroll_it = 0; unroll_it < unroll_block; unroll_it++)
                    {
                        if (add_all_elements || !condition_map[i + unroll_it] == add_if_cond_zero)
                            partial_min[unroll_it] = ComparatorClass::cmp(partial_min[unroll_it], ptr[i + unroll_it]);
                    }
                    i += unroll_block;
                }
                for (size_t unroll_it = 0; unroll_it < unroll_block; unroll_it++)
                    ret = ComparatorClass::cmp(ret, partial_min[unroll_it]);
            }

            for (; i < count; i++)
            {
                if (add_all_elements || !condition_map[i] == add_if_cond_zero)
                    ret = ComparatorClass::cmp(ret, ptr[i]);
            }
            return ret;
        }
        else
        {
            /// Only native integers
            for (; i < count; i++)
            {
                constexpr bool is_min = std::same_as<ComparatorClass, MinComparator<T>>;
                if constexpr (add_all_elements)
                {
                    ret = ComparatorClass::cmp(ret, ptr[i]);
                }
                else if constexpr (is_min)
                {
                    /// keep_number will be 0 or 1
                    bool keep_number = !condition_map[i] == add_if_cond_zero;
                    /// If keep_number = ptr[i] * 1 + 0 * max = ptr[i]
                    /// If not keep_number = ptr[i] * 0 + 1 * max = max
                    T final = ptr[i] * T{keep_number} + T{!keep_number} * std::numeric_limits<T>::max();
                    ret = ComparatorClass::cmp(ret, final);
                }
                else
                {
                    static_assert(std::same_as<ComparatorClass, MaxComparator<T>>);
                    /// keep_number will be 0 or 1
                    bool keep_number = !condition_map[i] == add_if_cond_zero;
                    /// If keep_number = ptr[i] * 1 + 0 * lowest = ptr[i]
                    /// If not keep_number = ptr[i] * 0 + 1 * lowest = lowest
                    T final = ptr[i] * T{keep_number} + T{!keep_number} * std::numeric_limits<T>::lowest();
                    ret = ComparatorClass::cmp(ret, final);
                }
            }
            return ret;
        }
    }
))

/// Given a vector of T finds the extreme (MIN or MAX) value
template <has_find_extreme_implementation T, class ComparatorClass, bool add_all_elements, bool add_if_cond_zero>
static std::optional<T>
findExtreme(const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end)
{
#if USE_MULTITARGET_CODE
    /// In some cases the compiler if able to apply the condition and still generate SIMD, so we still build both
    /// conditional and unconditional functions with multiple architectures
    /// We see no benefit from using AVX512BW or AVX512F (over AVX2), so we only declare SSE and AVX2
    if (isArchSupported(TargetArch::AVX2))
        return findExtremeImplAVX2<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);

    if (isArchSupported(TargetArch::SSE42))
        return findExtremeImplSSE42<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);
#endif
    return findExtremeImpl<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);
}

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, true, false>(ptr, nullptr, start, end);
}

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, false, true>(ptr, condition_map, start, end);
}

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, false, false>(ptr, condition_map, start, end);
}

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, true, false>(ptr, nullptr, start, end);
}

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, false, true>(ptr, condition_map, start, end);
}

template <has_find_extreme_implementation T>
std::optional<T> findExtremeMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, false, false>(ptr, condition_map, start, end);
}

template <has_find_extreme_implementation T>
std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end)
{
    /// This is implemented based on findNumericExtreme and not the other way around (or independently) because getting
    /// the MIN or MAX value of an array is possible with SIMD, but getting the index isn't.
    /// So what we do is use SIMD to find the lowest value and then iterate again over the array to find its position
    std::optional<T> opt = findExtremeMin(ptr, start, end);
    if (!opt)
        return std::nullopt;

    /// Some minimal heuristics for the case the input is sorted
    if (*opt == ptr[start])
        return {start};
    for (size_t i = end - 1; i > start; i--)
        if (ptr[i] == *opt)
            return {i};
    return std::nullopt;
}

template <has_find_extreme_implementation T>
std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end)
{
    std::optional<T> opt = findExtremeMax(ptr, start, end);
    if (!opt)
        return std::nullopt;

    /// Some minimal heuristics for the case the input is sorted
    if (*opt == ptr[start])
        return {start};
    for (size_t i = end - 1; i > start; i--)
        if (ptr[i] == *opt)
            return {i};
    return std::nullopt;
}

#define INSTANTIATION(T) \
    template std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findExtremeMinNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMinIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end);

FOR_BASIC_NUMERIC_TYPES(INSTANTIATION)
#undef INSTANTIATION

/*
DECLARE_AVX512VL_SPECIFIC_CODE(
template <bool is_min, bool add_all_elements, bool add_if_cond_zero>
static std::optional<UInt256>
findExtremeUInt256(const UInt256 * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end)
{
    const size_t count = end - start;
    ptr += start;
    if constexpr (!add_all_elements)
        condition_map += start;

    UInt256 ret{};
    size_t i = 0;
    for (; i < count; i++)
    {
        if (add_all_elements || !condition_map[i] == add_if_cond_zero)
        {
            ret = ptr[i];
            break;
        }
    }
    if (i >= count)
        return std::nullopt;

    const size_t prefetch_distance = 4;
    __mmask8 mask = 0xFF;
    __m256i extreme = _mm256_loadu_epi64(reinterpret_cast<const __m256i *>(&ret));
    for (; i < count; ++i)
    {
        if (i + prefetch_distance < count)
            _mm_prefetch(reinterpret_cast<const char *>(&ptr[i + prefetch_distance]), _MM_HINT_T0);

        if constexpr (add_all_elements)
        {
            __m256i candidate = _mm256_loadu_epi64(reinterpret_cast<const __m256i *>(&ptr[i]));
            __mmask8 less = _mm256_cmplt_epu64_mask(extreme, candidate);
            __mmask8 greater = _mm256_cmpgt_epu64_mask(extreme, candidate);
            bool candidate_is_greater = less > greater;
            if constexpr (is_min)
                extreme = _mm256_mask_blend_epi64(mask * (!candidate_is_greater), extreme, candidate);
            else
                extreme = _mm256_mask_blend_epi64(mask * (!!candidate_is_greater), extreme, candidate);
        }
    }
    _mm256_storeu_epi64(reinterpret_cast<__m256i *>(&ret), extreme);
    return ret;
}
)
*/

MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(
        template <bool is_min>
        static std::optional<UInt256> NO_INLINE),
    findExtremeUInt256Impl,
    MULTITARGET_FUNCTION_BODY((const UInt256 * __restrict ptr, UInt8 * __restrict mask, size_t row_begin, size_t row_end) /// NOLINT
    {
        UInt256 result{};
        for (unsigned i = 0; i < 4; ++i)
        {
            unsigned item_idx = UInt256::_impl::little(i);

            UInt64 extreme_item = 0;
            size_t row_idx = row_begin;
            for (; row_idx < row_end; ++row_idx)
            {
                if (mask[row_idx])
                {
                    extreme_item = ptr[row_idx].items[item_idx];
                    break;
                }
            }

            if (row_idx >= row_end)
                return std::nullopt;

            size_t next_j = row_idx;
            for (row_idx = next_j; row_idx < row_end; ++row_idx)
            {
                if constexpr (is_min)
                {
                    UInt64 final = ptr[row_idx].items[item_idx] * (!!mask[row_idx]) + (!mask[row_idx]) * std::numeric_limits<UInt64>::max();
                    extreme_item = std::min(extreme_item, final);
                }
                else
                {

                    UInt64 final = ptr[row_idx].items[item_idx] * (!!mask[row_idx]) + (!mask[row_idx]) * std::numeric_limits<UInt64>::lowest();
                    extreme_item = std::max(extreme_item, final);
                }
            }
            result.items[item_idx] = extreme_item;

            for (row_idx = next_j; row_idx < row_end; ++row_idx)
            {
                mask[row_idx] &= (extreme_item == ptr[row_idx].items[item_idx]);
            }
        }
        return result;
    }
))


template <typename T, bool is_min>
static std::optional<T>
findExtremeBigInt(const T * __restrict ptr, UInt8 * __restrict mask, size_t start, size_t end)
requires(is_big_int_v<T>)
{
#if USE_MULTITARGET_CODE
        if constexpr (std::is_same_v<T, UInt256>)
        {
            if (isArchSupported(TargetArch::AVX2))
            {
                std::cout << "xxxx" << std::endl;
                return findExtremeUInt256ImplAVX2<is_min>(ptr, mask, start, end);
            }

            if (isArchSupported(TargetArch::SSE42))
            {
                std::cout << "xxxx" << std::endl;
                return findExtremeUInt256ImplSSE42<is_min>(ptr, mask, start, end);
            }
        }
#endif
    return std::nullopt;
}

template <typename T>
requires(is_big_int_v<T>)
std::optional<T> findExtremeMinBigInt(const T * __restrict ptr, UInt8 * __restrict mask, size_t start, size_t end)
{
    return findExtremeBigInt<T, true>(ptr, mask, start, end);
}

template <typename T>
requires(is_big_int_v<T>)
std::optional<T> findExtremeMaxBigInt(const T * __restrict ptr, UInt8 * __restrict mask, size_t start, size_t end)
{
    return findExtremeBigInt<T, false>(ptr, mask, start, end);

}

#define INSTANTIATION(T) \
    template std::optional<T> findExtremeMinBigInt(const T * __restrict ptr, UInt8 * __restrict mask, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxBigInt(const T * __restrict ptr, UInt8 * __restrict mask, size_t start, size_t end);

INSTANTIATION(Int128)
INSTANTIATION(Int256)
INSTANTIATION(UInt128)
INSTANTIATION(UInt256)
#undef INSTANTIATION

}
