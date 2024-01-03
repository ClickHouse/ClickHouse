#include <DataTypes/IDataType.h>
#include <Common/TargetSpecific.h>
#include <Common/findExtreme.h>

namespace DB
{

template <is_any_native_number T>
struct MinComparator
{
    static ALWAYS_INLINE inline const T & cmp(const T & a, const T & b) { return std::min(a, b); }
};

template <is_any_native_number T>
struct MaxComparator
{
    static ALWAYS_INLINE inline const T & cmp(const T & a, const T & b) { return std::max(a, b); }
};

MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(template <is_any_native_number T, typename ComparatorClass, bool add_all_elements, bool add_if_cond_zero> static std::optional<T> NO_INLINE),
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
        }

        for (; i < count; i++)
        {
            if (add_all_elements || !condition_map[i] == add_if_cond_zero)
                ret = ComparatorClass::cmp(ret, ptr[i]);
        }

        return ret;
    }
))

/// Given a vector of T finds the extreme (MIN or MAX) value
template <is_any_native_number T, class ComparatorClass, bool add_all_elements, bool add_if_cond_zero>
static std::optional<T>
findExtreme(const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end)
{
#if USE_MULTITARGET_CODE
    /// We see no benefit from using AVX512BW or AVX512F (over AVX2), so we only declare SSE and AVX2
    if (isArchSupported(TargetArch::AVX2))
        return findExtremeImplAVX2<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);

    if (isArchSupported(TargetArch::SSE42))
        return findExtremeImplSSE42<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);
#endif
    return findExtremeImpl<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);
}

template <is_any_native_number T>
std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, true, false>(ptr, nullptr, start, end);
}

template <is_any_native_number T>
std::optional<T> findExtremeMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, false, true>(ptr, condition_map, start, end);
}

template <is_any_native_number T>
std::optional<T> findExtremeMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, false, false>(ptr, condition_map, start, end);
}

template <is_any_native_number T>
std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, true, false>(ptr, nullptr, start, end);
}

template <is_any_native_number T>
std::optional<T> findExtremeMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, false, true>(ptr, condition_map, start, end);
}

template <is_any_native_number T>
std::optional<T> findExtremeMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, false, false>(ptr, condition_map, start, end);
}


#define INSTANTIATION(T) \
    template std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findExtremeMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

FOR_BASIC_NUMERIC_TYPES(INSTANTIATION)
#undef INSTANTIATION
}
