#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>

#include <Columns/IColumn.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/DecomposedFloat.h>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramPayloadColumnsView;

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


int TimeSeriesDuplicateTimestampRule::compareHistograms(const View & view, size_t lhs, size_t rhs)
{
    if (const int by_count = compareCounts(view, lhs, rhs))
        return by_count;
    if (const int by_sum = view.sum.compareAt(lhs, rhs, view.sum, nan_is_less))
        return by_sum;

    /// The remaining elements in the stored order, each with the comparison of its column (arrays and tuples lexicographically).
    /// The counts and the sum are equal or were decided above.
    for (const IColumn * column : view.getPayloadColumnsInStoredOrder())
    {
        if (column == &view.counter_reset_hint)
        {
            /// Merging changes the counter hints, so a winner they decided would depend on the order of the merges. The gauge marker
            /// describes the sample itself and is never changed.
            const bool lhs_gauge = view.counter_reset_hint.getData()[lhs] == TimeSeriesHistogramCounterResetHint::Gauge;
            const bool rhs_gauge = view.counter_reset_hint.getData()[rhs] == TimeSeriesHistogramCounterResetHint::Gauge;
            if (lhs_gauge != rhs_gauge)
                return lhs_gauge ? 1 : -1;
            continue;
        }
        if (const int result = column->compareAt(lhs, rhs, *column, nan_is_less))
            return result;
    }
    return 0;
}

}
