#pragma once

#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/sliceEqualElements.h>
#include <base/defines.h>

#include <cstring>
#include <type_traits>
#include <utility>

namespace DB::GatherUtils
{

inline ALWAYS_INLINE bool hasNull(const UInt8 * null_map, size_t null_map_size)
{
    if (null_map == nullptr)
        return false;

    /// Without an early exit the loop is a few vector ORs. With one it was a scalar loop over every byte, which took a
    /// fifth of `hasAll` on a `Nullable` array that has no nulls.
    UInt8 any_null = 0;
    for (size_t i = 0; i < null_map_size; ++i)
        any_null |= null_map[i];

    return any_null != 0;
}

template <typename T>
constexpr bool is_integral_slice = false;

template <typename T>
requires std::is_integral_v<T>
constexpr bool is_integral_slice<NumericArraySlice<T>> = true;

/// Comparison results are accumulated in this view of the same bits, so that the tests below lower to a single `vptest`
/// on x86. Other spellings were slower: `reduce_and` over 1-byte lanes is a shuffle tree, and `reduce_max` over 4-byte
/// lanes narrows 8-byte masks with extra shuffles and blends in every test.
template <size_t bytes>
using SearchMask = UInt64 __attribute__((vector_size(bytes)));

template <typename Mask>
ALWAYS_INLINE bool anyLaneSet(Mask mask)
{
    return __builtin_reduce_or(mask) != 0;
}

template <typename Mask>
ALWAYS_INLINE bool allLanesSet(Mask mask)
{
    return __builtin_reduce_and(mask) == ~0ULL;
}

/// Searches `value` in blocks of 128 bytes. A block is a few whole-vector comparisons and one test of the combined mask,
/// so the search exits early without a branch per element. A 32-byte vector is one AVX2 register or two NEON registers.
template <bool has_null_map, typename T>
ALWAYS_INLINE bool sliceContains(const NumericArraySlice<T> & slice, const UInt8 * null_map, T value)
{
    using Vector = T __attribute__((vector_size(32)));
    using NullBytes = UInt8 __attribute__((vector_size(32 / sizeof(T))));
    static constexpr size_t lanes = 32 / sizeof(T);
    static constexpr size_t block_size = 4 * lanes;

    /// Filled lane by lane because comparing with the scalar directly is an implicit conversion that `-Wcharacter-conversion`
    /// rejects for `char8_t`, which is `UInt8`. It compiles to the same broadcast.
    Vector needle{};
    for (size_t lane = 0; lane < lanes; ++lane)
        needle[lane] = value;

    /// Without `ALWAYS_INLINE` the lambda can stay a call in the probe of `sliceHasAllBytes`.
    auto block_contains = [&](size_t begin) ALWAYS_INLINE
    {
        SearchMask<32> found{};
        /// The loop counts from zero so that its trip count is a constant and it is fully unrolled. With `begin + block_size`
        /// as the bound, the compiler cannot rule out an overflow and on AArch64 rebuilt the mask lane by lane.
        for (size_t offset = 0; offset < block_size; offset += lanes)
        {
            Vector elements;
            memcpy(&elements, slice.data + begin + offset, sizeof(elements));
            auto equal = elements == needle;
            if constexpr (has_null_map)
            {
                NullBytes nulls;
                memcpy(&nulls, null_map + begin + offset, sizeof(nulls));
                /// Subtracting one turns the flags into masks; comparing them with zero instead made x86 narrow the 8-byte
                /// comparison results to 4 bytes with two extra shuffles per vector.
                equal &= __builtin_convertvector(nulls, decltype(equal)) - 1;
            }
            found |= reinterpret_cast<SearchMask<32>>(equal);
        }
        return anyLaneSet(found);
    };

    size_t i = 0;
    for (; i + block_size <= slice.size; i += block_size)
    {
        if (block_contains(i))
            return true;
    }

    if (i == slice.size)
        return false;

    /// The last block overlaps the previous one; checking an element twice does not change the result.
    if (slice.size >= block_size)
        return block_contains(slice.size - block_size);

    bool found = false;
    for (; i < slice.size; ++i)
        found |= (slice.data[i] == value) & (!has_null_map || !null_map[i]);
    return found;
}

template <ArraySearchType search_type, bool has_first_null_map, typename T>
bool sliceHasIntegralImpl(const NumericArraySlice<T> & first, const NumericArraySlice<T> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    for (size_t i = 0; i < second.size; ++i)
    {
        if (second_null_map && second_null_map[i])
            continue;

        const bool has = sliceContains<has_first_null_map>(first, first_null_map, second.data[i]);

        if (has && search_type == ArraySearchType::Any)
            return true;

        if (!has && search_type == ArraySearchType::All)
            return false;
    }

    return search_type == ArraySearchType::All;
}

template <size_t shift, typename Vector, size_t... lane>
ALWAYS_INLINE Vector rotateLanes(Vector vector, std::index_sequence<lane...>)
{
    return __builtin_shufflevector(vector, vector, ((lane + shift) % sizeof...(lane))...);
}

/// `hasAll` for 1-byte elements. Values repeat often, so `sliceContains` finds each one within a few elements and its fixed
/// cost per value dominates. Instead, compares 16 values of `second` at once with all 16 rotations of each 16 elements of
/// `first`, and exits when all 16 are found. The vectors are 16 bytes because a rotation within one is a single instruction
/// (`palignr` on x86, `ext` on NEON), while rotating a 32-byte AVX2 vector crosses its 128-bit halves.
template <bool has_first_null_map, typename T>
requires (sizeof(T) == 1)
bool sliceHasAllBytes(const NumericArraySlice<T> & first, const NumericArraySlice<T> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    using Vector = T __attribute__((vector_size(16)));
    using NullBytes = UInt8 __attribute__((vector_size(16)));
    static constexpr size_t lanes = 16;
    static constexpr auto lane_indices = std::make_index_sequence<lanes>{};

    chassert(first.size >= lanes && second.size >= lanes);

    /// Null elements of `first` are replaced with a non-null element of `first`, so they cannot match anything that is not
    /// there anyway. That is one blend per vector instead of masking each of the 16 rotations.
    Vector fillers{};
    if constexpr (has_first_null_map)
    {
        const UInt8 * non_null = static_cast<const UInt8 *>(memchr(first_null_map, 0, first.size));
        if (!non_null)
            return sliceHasIntegralImpl<ArraySearchType::All, true>(first, second, first_null_map, second_null_map);
        for (size_t lane = 0; lane < lanes; ++lane)
            fillers[lane] = first.data[non_null - first_null_map];
    }

    auto compare_with_rotations = [&](SearchMask<16> & found, const Vector & values, size_t begin) ALWAYS_INLINE
    {
        Vector elements;
        memcpy(&elements, first.data + begin, sizeof(elements));
        if constexpr (has_first_null_map)
        {
            NullBytes nulls;
            memcpy(&nulls, first_null_map + begin, sizeof(nulls));
            elements = nulls == 0 ? elements : fillers;
        }
        /// The comparison masks are combined with an unsigned maximum, which is an OR for lanes that are 0 or 0xFF. With
        /// `|`, LLVM merged the 16 masks into booleans and rebuilt the bytes from them in every iteration (`psllw` and
        /// `pcmpgtb` on x86).
        using Bytes = UInt8 __attribute__((vector_size(16)));
        Bytes hits[lanes];
        [&]<size_t... shift>(std::index_sequence<shift...>) ALWAYS_INLINE
        {
            ((hits[shift] = reinterpret_cast<Bytes>(values == rotateLanes<shift>(elements, lane_indices))), ...);
        }(lane_indices);
        /// Combined as a tree: a chain of 16 dependent maximums limited AArch64 to one per `umax` latency.
        for (size_t width = lanes / 2; width > 0; width /= 2)
        {
            for (size_t i = 0; i < width; ++i)
                hits[i] = __builtin_elementwise_max(hits[i], hits[i + width]);
        }
        found = reinterpret_cast<SearchMask<16>>(__builtin_elementwise_max(reinterpret_cast<Bytes>(found), hits[0]));
    };

    auto group_found = [&](size_t begin) ALWAYS_INLINE
    {
        Vector values;
        memcpy(&values, second.data + begin, sizeof(values));

        /// Null values of `second` count as found: the caller has checked that `first` has a null.
        SearchMask<16> found{};
        if (second_null_map)
        {
            NullBytes nulls;
            memcpy(&nulls, second_null_map + begin, sizeof(nulls));
            found = reinterpret_cast<SearchMask<16>>(nulls != 0);
        }

        size_t i = 0;
        for (; i + lanes <= first.size; i += lanes)
        {
            compare_with_rotations(found, values, i);
            if (allLanesSet(found))
                return true;
        }

        /// The last vector overlaps the previous one; checking an element twice does not change the result.
        if (i < first.size)
            compare_with_rotations(found, values, first.size - lanes);

        return allLanesSet(found);
    };

    /// When a value is missing, it is usually the first one, and `sliceContains` finds that faster than a group.
    if (!(second_null_map && second_null_map[0]) && !sliceContains<has_first_null_map>(first, first_null_map, second.data[0]))
        return false;

    size_t i = 0;
    for (; i + lanes <= second.size; i += lanes)
    {
        if (!group_found(i))
            return false;
    }

    /// The last group overlaps the previous one.
    return i == second.size || group_found(second.size - lanes);
}

template <ArraySearchType search_type, bool has_first_null_map, typename T>
bool sliceHasIntegral(const NumericArraySlice<T> & first, const NumericArraySlice<T> & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    if constexpr (search_type == ArraySearchType::All && sizeof(T) == 1)
    {
        if (first.size >= 16 && second.size >= 16)
            return sliceHasAllBytes<has_first_null_map>(first, second, first_null_map, second_null_map);
    }
    return sliceHasIntegralImpl<search_type, has_first_null_map>(first, second, first_null_map, second_null_map);
}

/// Methods to check if first array has elements from second array, overloaded for various combinations of types.
template <
    ArraySearchType search_type,
    typename FirstSliceType,
    typename SecondSliceType,
    bool (*isEqual)(const FirstSliceType &, const SecondSliceType &, size_t, size_t)>
bool sliceHasImplAnyAll(const FirstSliceType & first, const SecondSliceType & second, const UInt8 * first_null_map, const UInt8 * second_null_map)
{
    const bool has_first_null_map = first_null_map != nullptr;
    const bool has_second_null_map = second_null_map != nullptr;

    const bool has_second_null = hasNull(second_null_map, second.size);
    if (has_second_null)
    {
        const bool has_first_null = hasNull(first_null_map, first.size);

        if (has_first_null && search_type == ArraySearchType::Any)
            return true;

        if (!has_first_null && search_type == ArraySearchType::All)
            return false;
    }

    if constexpr (std::is_same_v<FirstSliceType, SecondSliceType> && is_integral_slice<FirstSliceType>)
    {
        if (has_first_null_map)
            return sliceHasIntegral<search_type, true>(first, second, first_null_map, second_null_map);
        return sliceHasIntegral<search_type, false>(first, second, first_null_map, second_null_map);
    }

    for (size_t i = 0; i < second.size; ++i)
    {
        if (has_second_null_map && second_null_map[i])
            continue;

        bool has = false;

        for (size_t j = 0; j < first.size && !has; ++j)
        {
            if (has_first_null_map && first_null_map[j])
                continue;

            if (isEqual(first, second, j, i))
            {
                has = true;
                break;
            }
        }

        if (has && search_type == ArraySearchType::Any)
            return true;

        if (!has && search_type == ArraySearchType::All)
            return false;
    }

    return search_type == ArraySearchType::All;
}

}
