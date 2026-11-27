#pragma once

#include <Common/NaNUtils.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/bit_cast.h>
#include <base/types.h>

#include <cmath>


namespace DB
{

/// The rules deciding which of two samples of one time series with the same timestamp is kept. TimeSeriesSink applies them within
/// one INSERT, the aggregate function `timeSeriesGroupArray` across the rows of the samples tables, when parts are merged or read.
/// Both must apply the same rule, or the stored result would depend on whether the duplicates arrived in one block or in two.
/// The histogram rule ignores the counter hints, which merging changes, and compares only the gauge marker, which merging keeps.
/// Which of two samples equal under the rule is kept doesn't change the stored hints either: a sample with a predecessor has the hint
/// detected against it, the first sample of an array an unknown one unless it is a gauge (see TimeSeriesHistogramCounterResetRule).
namespace TimeSeriesDuplicateTimestampRule
{
    /// An order over float values: the greater one is preferred. Returns a negative number, 0 or a positive number; 0 only for
    /// values with the same bits. A NaN is below any number and two NaNs compare by their bits; `-0` is below `0`.
    template <typename ValueType>
    int compareFloats(ValueType lhs, ValueType rhs)
    {
        const bool lhs_nan = isNaN(lhs);
        const bool rhs_nan = isNaN(rhs);
        if (lhs_nan != rhs_nan)
            return lhs_nan ? -1 : 1;
        /// Of two NaNs the greater bit pattern wins, so a quiet NaN beats the Prometheus stale marker 0x7ff0000000000002.
        if (lhs_nan)
        {
            const UInt64 lhs_bits = bit_cast<UInt64>(lhs);
            const UInt64 rhs_bits = bit_cast<UInt64>(rhs);
            if (lhs_bits != rhs_bits)
                return (lhs_bits < rhs_bits) ? -1 : 1;
            return 0;
        }
        if (lhs != rhs)
            return (lhs < rhs) ? -1 : 1;
        /// Of +0 and -0 the +0 wins, so equal values also resolve independently of order.
        const bool lhs_negative = std::signbit(lhs);
        const bool rhs_negative = std::signbit(rhs);
        if (lhs_negative != rhs_negative)
            return lhs_negative ? -1 : 1;
        return 0;
    }

    /// The kept one of the float samples `kept` and `candidate` with the same timestamp. The result doesn't depend on the order of
    /// the arguments.
    template <typename ValueType>
    ValueType floatSampleWinner(ValueType kept, ValueType candidate)
    {
        return (compareFloats(candidate, kept) > 0) ? candidate : kept;
    }

    /// An order over the payload (every element but `timestamp`) of the histogram samples `lhs` and `rhs` of `view`: the greater
    /// sample is preferred. Returns a negative number, 0 or a positive number. The first differing key decides:
    /// 1. a stale marker is less than any non-stale sample;
    /// 2. the total count (`count_int` or `count_float` by flavour), exactly across flavours: an integer count as the float of the
    ///    same value;
    /// 3. `sum`;
    /// 4. the other elements in the stored order (arrays and tuples lexicographically); of the counter reset hint only the gauge
    ///    marker, so samples differing in their counter hints only are equal.
    /// Float elements are compared with `compareFloats`, so samples are equal only if their payloads have the same bits, the
    /// counter hints aside.
    int compareHistogramSamples(const TimeSeriesHistogramPayloadColumnsView & view, size_t lhs, size_t rhs);

    /// The same order for the sample `lhs` of `lhs_view` and the sample `rhs` of `rhs_view`.
    int compareHistogramSamples(
        const TimeSeriesHistogramPayloadColumnsView & lhs_view, size_t lhs, const TimeSeriesHistogramPayloadColumnsView & rhs_view, size_t rhs);
}

}
