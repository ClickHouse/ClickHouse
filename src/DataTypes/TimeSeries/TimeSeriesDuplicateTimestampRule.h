#pragma once

#include <Common/NaNUtils.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/types.h>

#include <type_traits>


namespace DB
{

/// The rules deciding which of two samples of one time series with the same timestamp is kept. TimeSeriesSink applies them to the
/// duplicates within one INSERT, the aggregate function `timeSeriesGroupArray` (for float samples through
/// `timeseriesMaxValueForDuplicateTimestamp`) to the duplicates across the rows of the samples tables, when parts are merged or read.
/// Both must apply the same rule, or the stored result would depend on whether the duplicates arrived in one block or in two.
/// The histogram rule doesn't compare the counter reset hints, which merging changes (see TimeSeriesHistogramCounterResetRule); of
/// the hints only the gauge marker, which merging keeps, takes part. Where the kept one of two samples equal under the rule keeps
/// its hint, the callers make it unknown unless the two hints agree, so that it doesn't depend on which one was kept.
class TimeSeriesDuplicateTimestampRule
{
public:
    /// Whether the float sample `candidate` replaces the float sample `kept`: the greatest value is kept, a NaN loses to any number,
    /// the stale marker to any other value, and on equal values the kept one stays. The rule of
    /// `timeseriesMaxValueForDuplicateTimestamp` in predicate form.
    template <typename ValueType>
    static bool floatSampleWins(ValueType kept, ValueType candidate)
    {
        if (isNaN(candidate))
        {
            /// A `Float32` value can't hold the stale marker.
            if constexpr (std::is_same_v<ValueType, Float64>)
                return TimeSeriesHistogramCounterResetRule::isStaleMarker(kept)
                    && !TimeSeriesHistogramCounterResetRule::isStaleMarker(candidate);
            else
                return false;
        }
        return isNaN(kept) || (candidate > kept);
    }

    /// An order over the payload (every element but `timestamp`) of the histogram samples `lhs` and `rhs` of `view`: the greater
    /// sample is preferred. Returns a negative number, 0 or a positive number. The first differing key decides:
    /// 1. a stale marker is less than any non-stale sample;
    /// 2. the total count (`count_int` or `count_float` by flavour), compared exactly across flavours;
    /// 3. `sum`;
    /// 4. the other elements in the stored order by `IColumn::compareAt` (arrays and tuples lexicographically); of the counter
    ///    reset hint only the gauge marker, so samples differing in their counter hints only are equal.
    /// Floats compare as in `CompareHelper`: a NaN is less than any number, two NaNs are equal, as are `0.0` and `-0.0`; only for
    /// payloads equal up to those does the kept sample depend on the order of the samples (the limit of `floatSampleWins` too).
    static int compareHistograms(const TimeSeriesHistogramPayloadColumnsView & view, size_t lhs, size_t rhs);
};

}
