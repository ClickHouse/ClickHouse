#include <DataTypes/IDataType.h>
#include <base/Decimal.h>
#include <Common/NaNUtils.h>
#include <Common/TargetSpecific.h>
#include <Common/findExtreme.h>

#include <limits>
#include <type_traits>

namespace DB
{

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
struct MinComparator
{
    static ALWAYS_INLINE inline const T & cmp(const T & a, const T & b) { return std::min(a, b); }
};

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
struct MaxComparator
{
    static ALWAYS_INLINE inline const T & cmp(const T & a, const T & b) { return std::max(a, b); }
};

namespace detail
{
template <class Comparator> struct NativeComparatorT { using Type = Comparator; };
template <underlying_has_find_extreme_implementation T> struct NativeComparatorT<MinComparator<T>> { using Type = MinComparator<NativeType<T>>; };
template <underlying_has_find_extreme_implementation T> struct NativeComparatorT<MaxComparator<T>> { using Type = MaxComparator<NativeType<T>>; };
template <class Comparator> using NativeComparator = typename NativeComparatorT<Comparator>::Type;
}

/// 128/256-bit values have no SIMD comparison, so the accumulator is updated behind a branch instead of a select,
/// and is kept behind an opaque pointer (hence NO_INLINE) so that it is compared in place and written only on an
/// update, rather than rotated through registers on every row.
template <typename T, bool is_min, bool add_all_elements, bool add_if_cond_zero>
static NO_INLINE void findExtremeWideImpl(
    const T * __restrict ptr,
    const UInt8 * __restrict condition_map [[maybe_unused]],
    size_t i,
    size_t count,
    T * __restrict accumulator)
{
    for (; i < count; i++)
    {
        if (add_all_elements || !condition_map[i] == add_if_cond_zero)
        {
            if constexpr (is_min)
            {
                if (ptr[i] < *accumulator)
                    *accumulator = ptr[i];
            }
            else
            {
                if (ptr[i] > *accumulator)
                    *accumulator = ptr[i];
            }
        }
    }
}

template <has_find_extreme_implementation T, typename ComparatorClass, bool add_all_elements, bool add_if_cond_zero>
static std::optional<T> findExtremeImpl(const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t row_begin, size_t row_end)
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
            /// For floats, skip NaN during initialisation so the accumulator starts with a non-NaN value.
            /// std::min/max never replace a NaN accumulator (NaN < x is always false), so a NaN start would stick through the loop.
            /// If all valid values are NaN, ret will hold the last NaN seen, which is correct.
            if constexpr (is_floating_point<T>)
            {
                if (!isNaN(ret))
                    break;
            }
            else
                break;
        }
    }
    if (i >= count)
    {
        /// For floats: if scanned all elements without finding a non-NaN, ret holds the last valid NaN. Return it (all values are NaN).
        if constexpr (is_floating_point<T>)
        {
            if (isNaN(ret))
                return ret;
        }
        return std::nullopt;
    }

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
    else if constexpr (is_big_int_v<T>)
    {
        constexpr bool is_min = std::same_as<ComparatorClass, MinComparator<T>>;
        findExtremeWideImpl<T, is_min, add_all_elements, add_if_cond_zero>(ptr, condition_map, i, count, &ret);
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


/// Given a vector of T finds the extreme (MIN or MAX) value
template <has_find_extreme_implementation T, class ComparatorClass, bool add_all_elements, bool add_if_cond_zero>
static std::optional<T>
findExtreme(const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end)
{
    return findExtremeImpl<T, ComparatorClass, add_all_elements, add_if_cond_zero>(ptr, condition_map, start, end);
}

template <underlying_has_find_extreme_implementation T, class ComparatorClass, bool add_all_elements, bool add_if_cond_zero>
static std::optional<T>
findExtreme(const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end)
{
    using U = NativeType<T>;
    auto ret = findExtreme<U, detail::NativeComparator<ComparatorClass>, add_all_elements, add_if_cond_zero>(
        reinterpret_cast<const U *>(ptr), condition_map, start, end);

    if (ret.has_value())
        return T(*ret);
    return std::nullopt;
}

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, true, false>(ptr, nullptr, start, end);
}

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, false, true>(ptr, condition_map, start, end);
}

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MinComparator<T>, false, false>(ptr, condition_map, start, end);
}

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, true, false>(ptr, nullptr, start, end);
}

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, false, true>(ptr, condition_map, start, end);
}

template <typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
std::optional<T> findExtremeMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
{
    return findExtreme<T, MaxComparator<T>, false, false>(ptr, condition_map, start, end);
}

/// Returns the first position in [start, end) holding `value` (any NaN matches a NaN `value`), or `end`.
template <typename T>
static size_t findFirstEqual(const T * __restrict ptr, size_t start, size_t end, T value)
{
    if constexpr (is_floating_point<T>)
    {
        if (isNaN(value))
        {
            for (size_t i = start; i < end; ++i)
                if (isNaN(ptr[i]))
                    return i;
            return end;
        }
    }

    /// Compare whole blocks without an early exit so that the comparison is vectorized
    constexpr size_t block_size = std::max<size_t>(8, 64 / sizeof(T));
    size_t i = start;
    for (; i + block_size <= end; i += block_size)
    {
        bool found = false;
        for (size_t j = 0; j < block_size; ++j)
            found |= ptr[i + j] == value;
        if (found)
            break;
    }
    for (; i < end; ++i)
        if (ptr[i] == value)
            return i;
    return end;
}

/// Getting the MIN or MAX value is possible with SIMD, but getting its index isn't, so we find the value first and then
/// search for its first occurrence. The value is found chunk by chunk, remembering the first chunk that reached it, so the
/// second scan is limited to a single chunk that is still in cache.
template <typename T, bool is_min>
static std::optional<size_t> findExtremeIndex(const T * __restrict ptr, size_t start, size_t end)
{
    using U = NativeType<T>;
    const U * __restrict data = reinterpret_cast<const U *>(ptr);

    constexpr size_t chunk_size = 8192;
    std::optional<U> best;
    size_t best_chunk_begin = start;
    for (size_t chunk_begin = start; chunk_begin < end; chunk_begin += chunk_size)
    {
        size_t chunk_end = std::min(end, chunk_begin + chunk_size);
        std::optional<U> value = is_min ? findExtremeMin(data, chunk_begin, chunk_end) : findExtremeMax(data, chunk_begin, chunk_end);
        chassert(value.has_value());

        bool better = !best || (is_min ? *value < *best : *value > *best);
        /// A chunk only returns NaN if all its values are NaN
        if constexpr (is_floating_point<U>)
            better = better || (isNaN(*best) && !isNaN(*value));
        if (better)
        {
            best = value;
            best_chunk_begin = chunk_begin;
        }
    }

    if (!best)
        return std::nullopt;

    size_t best_chunk_end = std::min(end, best_chunk_begin + chunk_size);
    size_t index = findFirstEqual(data, best_chunk_begin, best_chunk_end, *best);
    chassert(index < best_chunk_end);
    return index;
}

template <typename T>
requires(has_find_extreme_index_implementation<T>)
std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtremeIndex<T, true>(ptr, start, end);
}

template <typename T>
requires(has_find_extreme_index_implementation<T>)
std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end)
{
    return findExtremeIndex<T, false>(ptr, start, end);
}

#define INSTANTIATION_VALUE(T) \
    template std::optional<T> findExtremeMin(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findExtremeMinNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMinIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMax(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxNotNull( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findExtremeMaxIf( \
        const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

#define INSTANTIATION(T) \
    INSTANTIATION_VALUE(T) \
    template std::optional<size_t> findExtremeMinIndex(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<size_t> findExtremeMaxIndex(const T * __restrict ptr, size_t start, size_t end);

FOR_BASIC_NUMERIC_TYPES(INSTANTIATION)

INSTANTIATION(Decimal32)
INSTANTIATION(Decimal64)
INSTANTIATION(DateTime64)

INSTANTIATION_VALUE(Int128)
INSTANTIATION_VALUE(Int256)
INSTANTIATION_VALUE(UInt128)
INSTANTIATION_VALUE(UInt256)
INSTANTIATION_VALUE(Decimal128)
INSTANTIATION_VALUE(Decimal256)

#undef INSTANTIATION
#undef INSTANTIATION_VALUE
}
