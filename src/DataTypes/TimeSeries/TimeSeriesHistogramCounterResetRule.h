#pragma once

#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

/// The rules deciding the counter reset hint of a histogram sample from its predecessor in the series.
class TimeSeriesHistogramCounterResetRule
{
public:
    static bool isStaleMarker(Float64 sum);

    /// The hint of sample `current` of `view` given that sample `previous` is its predecessor in the series. With
    /// `preserve_explicit_reset`, a reset hint the sample was sent with is kept instead of being detected from the data.
    static UInt8 hintAfterPredecessor(
        const TimeSeriesHistogramPayloadColumnsView & view,
        size_t previous,
        size_t current,
        bool preserve_explicit_reset);

    /// The hint of sample `current` detected from its data and the data of its predecessor `previous`, ignoring the hint it was sent with.
    static UInt8 detect(const TimeSeriesHistogramPayloadColumnsView & view, size_t previous, size_t current);
};

}
