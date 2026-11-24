#pragma once

#include <Common/NaNUtils.h>
#include <DataTypes/TimeSeries/PrometheusStaleMarker.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/types.h>

#include <type_traits>


namespace DB
{

/// The rules deciding which of two samples of one time series with the same timestamp is kept. TimeSeriesSink applies them to the
/// duplicates within one INSERT, the aggregate function `timeSeriesGroupArray` (for float samples through `floatSampleWinner`) to the
/// duplicates across the rows of the samples tables, when parts are merged or read.
/// Both must apply the same rule, or the stored result would depend on whether the duplicates arrived in one block or in two.
/// The histogram rule doesn't compare the counter reset hints, which merging changes (see TimeSeriesHistogramCounterResetRule); of
/// the hints only the gauge marker, which merging keeps, takes part. Which of two samples equal under the rule is kept doesn't change
/// the stored hints either: a sample with a predecessor has the hint detected against it, the first sample of an array an unknown one
/// unless it is a gauge (see TimeSeriesHistogramCounterResetRule).
namespace TimeSeriesDuplicateTimestampRule
{
    /// An order over the float samples `lhs` and `rhs`: the greater sample is preferred. Returns a negative number, 0 or a positive
    /// number. The stale marker is less than any other NaN, a NaN less than any number; NaNs of one kind are equal, as are `0.0` and
    /// `-0.0`.
    template <typename ValueType>
    int compareFloatSamples(ValueType lhs, ValueType rhs)
    {
        const auto rank = [](ValueType value)
        {
            if (!isNaN(value))
                return 2;
            /// A `Float32` value can't hold the stale marker.
            if constexpr (std::is_same_v<ValueType, Float64>)
            {
                if (isPrometheusStaleMarker(value))
                    return 0;
            }
            return 1;
        };
        const int lhs_rank = rank(lhs);
        const int rhs_rank = rank(rhs);
        if (lhs_rank != rhs_rank)
            return (lhs_rank < rhs_rank) ? -1 : 1;
        if (lhs < rhs)
            return -1;
        return (lhs > rhs) ? 1 : 0;
    }

    /// The kept one of the float samples `kept` and `candidate` with the same timestamp; of equal samples `kept`.
    template <typename ValueType>
    ValueType floatSampleWinner(ValueType kept, ValueType candidate)
    {
        return (compareFloatSamples(candidate, kept) > 0) ? candidate : kept;
    }

    /// An order over the payload (every element but `timestamp`) of the histogram samples `lhs` and `rhs` of `view`: the greater
    /// sample is preferred. Returns a negative number, 0 or a positive number. The first differing key decides:
    /// 1. a stale marker is less than any non-stale sample;
    /// 2. the total count (`count_int` or `count_float` by flavour), compared exactly across flavours;
    /// 3. `sum`;
    /// 4. the other elements in the stored order by `IColumn::compareAt` (arrays and tuples lexicographically); of the counter
    ///    reset hint only the gauge marker, so samples differing in their counter hints only are equal.
    /// Floats compare as in `CompareHelper`: a NaN is less than any number, two NaNs are equal, as are `0.0` and `-0.0`; only for
    /// payloads equal up to those does the kept sample depend on the order of the samples (the limit of `compareFloatSamples` too).
    int compareHistogramSamples(const TimeSeriesHistogramPayloadColumnsView & view, size_t lhs, size_t rhs);

    /// The same order for the sample `lhs` of `lhs_view` and the sample `rhs` of `rhs_view`, views over columns of the same types.
    int compareHistogramSamples(
        const TimeSeriesHistogramPayloadColumnsView & lhs_view, size_t lhs, const TimeSeriesHistogramPayloadColumnsView & rhs_view, size_t rhs);
}

}
