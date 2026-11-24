#pragma once

#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <limits>


namespace DB
{

/// The rules deciding the counter reset hint of a histogram sample from its predecessor in the series.
namespace TimeSeriesHistogramCounterResetRule
{
    /// The `previous` of a sample without a predecessor in its array, the first sample of the array.
    constexpr size_t NO_PREVIOUS = std::numeric_limits<size_t>::max();

    /// The hint of sample `current` of `view` given that sample `previous` is its predecessor in the series, or `NO_PREVIOUS`: the
    /// gauge marker if the sample has one, otherwise the hint detected from the data, unknown without a predecessor.
    TimeSeriesHistogramCounterResetHint detectReset(
        const TimeSeriesHistogramPayloadColumnsView & view,
        size_t previous,
        size_t current);
}

}
