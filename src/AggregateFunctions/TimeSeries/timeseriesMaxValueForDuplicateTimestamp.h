#pragma once

#include <bit>
#include <type_traits>

#include <Common/NaNUtils.h>


namespace DB
{

/// Tie-break for values `==` cannot separate (two NaN payloads, or -0.0 against +0.0): raw bits, so the
/// surviving value does not depend on arrival or merge order.
template <typename ValueType>
bool timeseriesHasGreaterBits(ValueType lhs, ValueType rhs)
{
    using Bits = std::conditional_t<sizeof(ValueType) == sizeof(UInt32), UInt32, UInt64>;
    return std::bit_cast<Bits>(lhs) > std::bit_cast<Bits>(rhs);
}

/// Returns the larger of two values sharing a timestamp; a NaN loses to any real value, and values `==`
/// cannot separate (two NaN payloads, or -0.0 against +0.0) are decided by raw bits.
/// The operation is associative and commutative, so the result does not depend on arrival or merge order.
/// This is the common rule of the `timeSeries*` aggregate functions for duplicate timestamps.
template <typename ValueType>
ValueType timeseriesMaxValueForDuplicateTimestamp(ValueType lhs, ValueType rhs)
{
    if (isNaN(lhs))
    {
        if (!isNaN(rhs))
            return rhs;
    }
    else if (isNaN(rhs))
        return lhs;
    else if (lhs > rhs)
        return lhs;
    else if (rhs > lhs)
        return rhs;
    return timeseriesHasGreaterBits(lhs, rhs) ? lhs : rhs;
}

}
