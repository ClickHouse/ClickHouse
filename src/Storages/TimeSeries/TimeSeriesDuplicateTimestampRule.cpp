#include <Storages/TimeSeries/TimeSeriesDuplicateTimestampRule.h>

#include <Columns/IColumn.h>
#include <Core/CompareHelper.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramsColumnsView;

    /// `IColumn::compareAt` orders a NaN below any number with this hint, so a NaN count or sum loses to any other.
    constexpr int nan_is_less = -1;

    /// The first key: the total count, exact when both samples are of the same flavour, as `Float64` when they are not.
    int compareCounts(const View & view, size_t lhs, size_t rhs)
    {
        const bool lhs_float = view.isFloatAt(lhs);
        const bool rhs_float = view.isFloatAt(rhs);
        if (lhs_float != rhs_float)
            return CompareHelper<Float64>::compare(view.countAt(lhs), view.countAt(rhs), nan_is_less);
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
        if (const int result = column->compareAt(lhs, rhs, *column, nan_is_less))
            return result;
    }
    return 0;
}

}
