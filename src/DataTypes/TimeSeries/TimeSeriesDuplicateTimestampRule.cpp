#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>

#include <Columns/IColumn.h>
#include <DataTypes/TimeSeries/PrometheusStaleMarker.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/DecomposedFloat.h>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramPayloadColumnsView;

    /// `IColumn::compareAt` orders a NaN below any number with this hint, so a NaN count or sum loses to any other.
    constexpr int nan_is_less = -1;

    /// The second key: the total count, compared exactly even across flavours without rounding an integer to `Float64`.
    int compareCounts(const View & lhs_view, size_t lhs, const View & rhs_view, size_t rhs)
    {
        const bool lhs_float = lhs_view.isFloatAt(lhs);
        const bool rhs_float = rhs_view.isFloatAt(rhs);
        if (lhs_float != rhs_float)
        {
            const DecomposedFloat<Float64> float_count((lhs_float ? lhs_view : rhs_view).count_float.getData()[lhs_float ? lhs : rhs]);
            const Int64 int_count = (lhs_float ? rhs_view : lhs_view).count_int.getData()[lhs_float ? rhs : lhs];
            const int result = float_count.isNaN() ? nan_is_less : float_count.compare(int_count);
            return lhs_float ? result : -result;
        }
        if (lhs_float)
            return lhs_view.count_float.compareAt(lhs, rhs, rhs_view.count_float, nan_is_less);
        return lhs_view.count_int.compareAt(lhs, rhs, rhs_view.count_int, nan_is_less);
    }
}

namespace TimeSeriesDuplicateTimestampRule
{
int compareHistogramSamples(const View & view, size_t lhs, size_t rhs)
{
    return compareHistogramSamples(view, lhs, view, rhs);
}

int compareHistogramSamples(const View & lhs_view, size_t lhs, const View & rhs_view, size_t rhs)
{
    /// The other elements of a stale marker are ignored, so they must not let it beat another sample, e.g. by a greater count.
    const bool lhs_stale = isPrometheusStaleMarker(lhs_view.sum.getData()[lhs]);
    const bool rhs_stale = isPrometheusStaleMarker(rhs_view.sum.getData()[rhs]);
    if (lhs_stale != rhs_stale)
        return lhs_stale ? -1 : 1;
    if (const int by_count = compareCounts(lhs_view, lhs, rhs_view, rhs))
        return by_count;
    if (const int by_sum = lhs_view.sum.compareAt(lhs, rhs, rhs_view.sum, nan_is_less))
        return by_sum;

    /// The remaining elements in the stored order, each with the comparison of its column (arrays and tuples lexicographically).
    /// The counts and the sum are equal or were decided above.
    const auto lhs_columns = lhs_view.getPayloadColumnsInStoredOrder();
    const auto rhs_columns = rhs_view.getPayloadColumnsInStoredOrder();
    for (size_t position = 0; position != lhs_columns.size(); ++position)
    {
        if (lhs_columns[position] == &lhs_view.counter_reset_hint)
        {
            /// Merging changes the counter hints, so a winner they decided would depend on the order of the merges. The gauge marker
            /// describes the sample itself and is never changed.
            const bool lhs_gauge = lhs_view.counterResetHintAt(lhs) == TimeSeriesHistogramCounterResetHint::Gauge;
            const bool rhs_gauge = rhs_view.counterResetHintAt(rhs) == TimeSeriesHistogramCounterResetHint::Gauge;
            if (lhs_gauge != rhs_gauge)
                return lhs_gauge ? 1 : -1;
            continue;
        }
        if (const int result = lhs_columns[position]->compareAt(lhs, rhs, *rhs_columns[position], nan_is_less))
            return result;
    }
    return 0;
}
}

}
