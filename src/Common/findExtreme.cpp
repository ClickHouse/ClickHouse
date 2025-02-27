#include <DataTypes/IDataType.h>
#include <Common/TargetSpecific.h>
#include <Common/findExtreme.h>

#include <limits>
#include <type_traits>

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
        if constexpr (std::is_floating_point_v<T>)
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
}
