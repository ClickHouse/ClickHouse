#pragma once

#include <Common/NaNUtils.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/bit_cast.h>
#include <base/types.h>

#include <algorithm>
#include <cmath>


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
    /// The kept one of the float samples `kept` and `candidate` with the same timestamp. The result doesn't depend on the order of
    /// the arguments.
    template <typename ValueType>
    ValueType floatSampleWinner(ValueType kept, ValueType candidate)
    {
        /// Of two NaNs the greater bit pattern wins, so a quiet NaN beats the Prometheus stale marker 0x7ff0000000000002.
        if (isNaN(kept))
            return (isNaN(candidate) && bit_cast<UInt64>(kept) > bit_cast<UInt64>(candidate)) ? kept : candidate;
        if (isNaN(candidate))
            return kept;
        /// Of +0 and -0 the +0 wins, so equal values also resolve independently of order.
        if (kept == candidate)
            return std::signbit(kept) ? candidate : kept;
        return std::max(kept, candidate);
    }

    /// An order over the payload (every element but `timestamp`) of the histogram samples `lhs` and `rhs` of `view`: the greater
    /// sample is preferred. Returns a negative number, 0 or a positive number. The first differing key decides:
    /// 1. a stale marker is less than any non-stale sample;
    /// 2. the total count (`count_int` or `count_float` by flavour), compared exactly across flavours;
    /// 3. `sum`;
    /// 4. the other elements in the stored order, compared as `IColumn::compareAt` compares them (arrays and tuples lexicographically);
    ///    of the counter reset hint only the gauge marker, so samples differing in their counter hints only are equal.
    /// Floats compare as in `CompareHelper`: a NaN is less than any number, two NaNs are equal, as are `0.0` and `-0.0`; only for
    /// payloads equal up to those does the kept sample depend on the order of the samples.
    int compareHistogramSamples(const TimeSeriesHistogramPayloadColumnsView & view, size_t lhs, size_t rhs);

    /// The same order for the sample `lhs` of `lhs_view` and the sample `rhs` of `rhs_view`.
    int compareHistogramSamples(
        const TimeSeriesHistogramPayloadColumnsView & lhs_view, size_t lhs, const TimeSeriesHistogramPayloadColumnsView & rhs_view, size_t rhs);
}

}
