#pragma once

#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>


namespace DB
{

/// Returns the larger of two values sharing a timestamp; a NaN loses to any real value.
/// The operation is associative and commutative, so the result does not depend on arrival or merge order.
/// This is the common rule of the `timeSeries*` aggregate functions for duplicate timestamps, shared with
/// TimeSeriesSink through `TimeSeriesDuplicateTimestampRule::floatSampleWins`.
template <typename ValueType>
ValueType timeseriesMaxValueForDuplicateTimestamp(ValueType lhs, ValueType rhs)
{
    return TimeSeriesDuplicateTimestampRule::floatSampleWins(lhs, rhs) ? rhs : lhs;
}

}
