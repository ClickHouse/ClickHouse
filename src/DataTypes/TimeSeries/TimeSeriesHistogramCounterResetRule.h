#pragma once

#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

/// The rules deciding the counter reset hint of a histogram sample from its predecessor in the series.
namespace TimeSeriesHistogramCounterResetRule
{
    /// The hint of sample `current` of `view` given that sample `previous` is its predecessor in the series: the gauge marker
    /// if the sample has one, otherwise the hint detected from the data.
    TimeSeriesHistogramCounterResetHint detectReset(
        const TimeSeriesHistogramPayloadColumnsView & view,
        size_t previous,
        size_t current);
}

}
