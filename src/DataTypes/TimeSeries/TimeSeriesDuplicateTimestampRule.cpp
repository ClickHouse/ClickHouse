#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>

#include <Columns/IColumn.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/DecomposedFloat.h>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramsColumnsView;

    /// `IColumn::compareAt` orders a NaN below any number with this hint, so a NaN count or sum loses to any other.
    constexpr int nan_is_less = -1;

    /// The first key: the total count, compared exactly even across flavours without rounding an integer to `Float64`.
    int compareCounts(const View & view, size_t lhs, size_t rhs)
    {
        const bool lhs_float = view.isFloatAt(lhs);
        const bool rhs_float = view.isFloatAt(rhs);
        if (lhs_float != rhs_float)
        {
            const DecomposedFloat<Float64> float_count(view.count_float.getData()[lhs_float ? lhs : rhs]);
            const Int64 int_count = view.count_int.getData()[lhs_float ? rhs : lhs];
            const int result = float_count.isNaN() ? nan_is_less : float_count.compare(int_count);
            return lhs_float ? result : -result;
        }
        const IColumn & counts = lhs_float ? static_cast<const IColumn &>(view.count_float) : view.count_int;
        return counts.compareAt(lhs, rhs, counts, nan_is_less);
    }
}


bool TimeSeriesDuplicateTimestampRule::histogramSampleWins(const View & view, size_t kept, size_t candidate)
{
    return compareHistograms(view, candidate, kept) > 0;
}

int TimeSeriesDuplicateTimestampRule::compareHistograms(const View & view, size_t lhs, size_t rhs)
{
    if (const int by_count = compareCounts(view, lhs, rhs))
        return by_count;
    if (const int by_sum = view.sum.compareAt(lhs, rhs, view.sum, nan_is_less))
        return by_sum;

    /// The remaining elements in the stored order, each with the comparison of its column (arrays and tuples lexicographically).
    /// The timestamps are equal within a run of duplicates, and the counts and the sum are equal or were decided above.
    for (const IColumn * column : view.getColumnsInStoredOrder())
    {
        if (column == &view.counter_reset_hint)
        {
            /// Changing a counter hint during merging must not change which histogram payload wins a later merge.
            const bool lhs_gauge = view.counter_reset_hint.getData()[lhs] == TimeSeriesHistogramCounterResetHint::Gauge;
            const bool rhs_gauge = view.counter_reset_hint.getData()[rhs] == TimeSeriesHistogramCounterResetHint::Gauge;
            if (lhs_gauge != rhs_gauge)
                return lhs_gauge ? 1 : -1;
            continue;
        }
        if (const int result = column->compareAt(lhs, rhs, *column, nan_is_less))
            return result;
    }

    /// Only identical payloads are ordered by their hints. Prefer an unknown hint, so that a duplicate cannot replace a hint which
    /// merging made unknown with a known one.
    const UInt8 lhs_hint = view.counter_reset_hint.getData()[lhs];
    const UInt8 rhs_hint = view.counter_reset_hint.getData()[rhs];
    if (lhs_hint == rhs_hint)
        return 0;
    if (lhs_hint == TimeSeriesHistogramCounterResetHint::Unknown)
        return 1;
    if (rhs_hint == TimeSeriesHistogramCounterResetHint::Unknown)
        return -1;
    return lhs_hint > rhs_hint ? 1 : -1;
}

}
