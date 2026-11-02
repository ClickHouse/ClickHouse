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
/// The histogram rule orders the whole stored payload, so its result is also independent of the order of the parts in a merge
/// (up to payloads the comparison of ClickHouse considers equal, see `compareHistograms`).
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

    /// An order over the payload (every element but `timestamp`) of the histogram samples `lhs` and `rhs` of `view`; the greater
    /// sample is the preferred one. Returns a negative number, 0 or a positive number. The first differing key decides:
    /// 1. the total count: both integer histograms -> `count_int`, both float -> `count_float`, mixed -> both as `Float64`;
    /// 2. `sum`;
    /// 3. the other elements in the stored order, each by the comparison of its column (`IColumn::compareAt`: arrays and tuples
    ///    lexicographically, a proper prefix is less).
    /// Floats follow the comparison rules of ClickHouse (`CompareHelper`): a NaN is less than any number, two NaNs are equal, as are
    /// `0.0` and `-0.0`. So 0 means equal payloads up to those, and only then the kept sample depends on the order of the samples
    /// (the same limit as `floatSampleWins` has).
    static int compareHistograms(const TimeSeriesHistogramsColumnsView & view, size_t lhs, size_t rhs);
};

}
