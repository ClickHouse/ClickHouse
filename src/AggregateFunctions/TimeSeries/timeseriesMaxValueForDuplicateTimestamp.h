#pragma once

#include <algorithm>

#include <base/bit_cast.h>
#include <Common/NaNUtils.h>


namespace DB
{

/// Returns the larger of two values sharing a timestamp; a NaN loses to any real value.
/// The operation is associative and commutative, so the result does not depend on arrival or merge order.
/// This is the common rule of the `timeSeries*` aggregate functions for duplicate timestamps.
template <typename ValueType>
ValueType timeseriesMaxValueForDuplicateTimestamp(ValueType lhs, ValueType rhs)
{
    /// Of two NaNs the greater bit pattern wins, so an ordinary NaN beats the Prometheus stale marker 0x7ff0000000000002.
    if (isNaN(lhs))
        return (isNaN(rhs) && bit_cast<UInt64>(lhs) > bit_cast<UInt64>(rhs)) ? lhs : rhs;
    if (isNaN(rhs))
        return lhs;
    return std::max(lhs, rhs);
}

}
