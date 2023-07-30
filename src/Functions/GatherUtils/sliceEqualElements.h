#pragma once

#include <Core/AccurateComparison.h>
#include "Slices.h"

namespace DB::GatherUtils
{

template <typename T, typename U>
bool sliceEqualElements(const NumericArraySlice<T> & first [[maybe_unused]],
                        const NumericArraySlice<U> & second [[maybe_unused]],
                        size_t first_ind [[maybe_unused]],
                        size_t second_ind [[maybe_unused]])
{
    /// TODO: Decimal scale
    if constexpr (is_decimal<T> && is_decimal<U>)
        return accurate::equalsOp(first.data[first_ind].value, second.data[second_ind].value);
    else if constexpr (is_decimal<T> || is_decimal<U>)
        return false;
    else
        return accurate::equalsOp(first.data[first_ind], second.data[second_ind]);
}

template <typename T>
bool sliceEqualElements(const NumericArraySlice<T> &, const GenericArraySlice &, size_t, size_t)
{
    return false;
}

template <typename U>
bool sliceEqualElements(const GenericArraySlice &, const NumericArraySlice<U> &, size_t, size_t)
{
    return false;
}

inline ALWAYS_INLINE bool sliceEqualElements(const GenericArraySlice & first, const GenericArraySlice & second, size_t first_ind, size_t second_ind)
{
    return first.elements->compareAt(first_ind + first.begin, second_ind + second.begin, *second.elements, -1) == 0;
}

}
