#pragma once

#include <Common/NaNUtils.h>
#include <base/types.h>


namespace DB
{
class TimeSeriesHistogramsColumnsView;

/// The rules deciding which of two samples of one time series with the same timestamp is kept. TimeSeriesSink applies them to the
/// duplicates within one INSERT, the aggregate functions `timeSeriesGroupArray` (through `timeseriesMaxValueForDuplicateTimestamp`)
/// and `timeSeriesGroupArrayHistograms` to the duplicates across the rows of the samples tables, when parts are merged or read.
/// Both must apply the same rule, or the stored result would depend on whether the duplicates arrived in one block or in two.
/// The histogram rule compares counter reset hints only after the other payload elements, so changing a hint during merging
/// (see TimeSeriesHistogramCounterResetRule) cannot change the winning payload (up to payloads the comparison of ClickHouse
/// considers equal, see `compareHistograms`).
class TimeSeriesDuplicateTimestampRule
{
public:
    /// Whether the float sample `candidate` replaces the float sample `kept`: the greatest value is kept, a NaN loses to any other
    /// value, and on equal values the kept one stays. The rule of `timeseriesMaxValueForDuplicateTimestamp` in predicate form.
    template <typename ValueType>
    static bool floatSampleWins(ValueType kept, ValueType candidate)
    {
        if (isNaN(candidate))
            return false;
        return isNaN(kept) || (candidate > kept);
    }

    /// Whether the histogram sample `candidate` of `view` replaces the histogram sample `kept`: `compareHistograms(view, candidate, kept) > 0`.
    static bool histogramSampleWins(const TimeSeriesHistogramsColumnsView & view, size_t kept, size_t candidate);

    /// An order over the payload (every element but `timestamp`) of the histogram samples `lhs` and `rhs` of `view`: the greater
    /// sample is preferred. Returns a negative number, 0 or a positive number. The first differing key decides:
    /// 1. the total count (`count_int` or `count_float` by flavour), compared exactly across flavours;
    /// 2. `sum`;
    /// 3. the other elements in the stored order by `IColumn::compareAt` (arrays and tuples lexicographically); of the counter
    ///    reset hint only the gauge marker;
    /// 4. for otherwise identical payloads, the counter reset hint, unknown preferred over known hints.
    /// Floats compare as in `CompareHelper`: a NaN is less than any number, two NaNs are equal, as are `0.0` and `-0.0`; only for
    /// payloads equal up to those does the kept sample depend on the order of the samples (the limit of `floatSampleWins` too).
    static int compareHistograms(const TimeSeriesHistogramsColumnsView & view, size_t lhs, size_t rhs);
};

}
