#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>

#include <Core/CompareHelper.h>
#include <DataTypes/TimeSeries/PrometheusStaleMarker.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/DecomposedFloat.h>

#include <algorithm>
#include <span>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramPayloadColumnsView;
    using Element = TimeSeriesHistogramsColumn;

    /// `CompareHelper` orders a NaN below any number with this hint, so a NaN count or sum loses to any other.
    constexpr int nan_is_less = -1;

    /// The order of `IColumn::compareAt` over numbers.
    template <typename T>
    int compareNumbers(T lhs, T rhs)
    {
        return CompareHelper<T>::compare(lhs, rhs, nan_is_less);
    }

    template <typename T>
    int compareNumbersAt(const ColumnVector<T> & lhs_column, size_t lhs, const ColumnVector<T> & rhs_column, size_t rhs)
    {
        return compareNumbers(lhs_column.getData()[lhs], rhs_column.getData()[rhs]);
    }

    /// The order of `IColumn::compareAt` over arrays: by the first differing element, a prefix first. `compare_elements(i)` compares
    /// the elements `i` of both arrays.
    template <typename CompareElements>
    int compareArrays(size_t lhs_size, size_t rhs_size, const CompareElements & compare_elements)
    {
        const size_t common_size = std::min(lhs_size, rhs_size);
        for (size_t i = 0; i != common_size; ++i)
        {
            if (const int result = compare_elements(i))
                return result;
        }
        return compareNumbers(lhs_size, rhs_size);
    }

    /// The order of `IColumn::compareAt` over the buckets, arrays of tuples: a pair by the index, then by the count.
    template <typename Count>
    int compareBuckets(const View::Pairs<Count> & lhs, const View::Pairs<Count> & rhs)
    {
        return compareArrays(lhs.size(), rhs.size(), [&](size_t i)
        {
            if (const int by_index = compareNumbers(lhs.indexes[i], rhs.indexes[i]))
                return by_index;
            return compareNumbers(lhs.counts[i], rhs.counts[i]);
        });
    }

    int compareFloatArrays(std::span<const Float64> lhs, std::span<const Float64> rhs)
    {
        return compareArrays(lhs.size(), rhs.size(), [&](size_t i) { return compareNumbers(lhs[i], rhs[i]); });
    }

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
            return compareNumbersAt(lhs_view.count_float, lhs, rhs_view.count_float, rhs);
        return compareNumbersAt(lhs_view.count_int, lhs, rhs_view.count_int, rhs);
    }

    /// The samples `lhs` of `lhs_view` and `rhs` of `rhs_view` by `element` in the order of `IColumn::compareAt`, the counter reset
    /// hints by the gauge marker only.
    int compareByElement(Element element, const View & lhs_view, size_t lhs, const View & rhs_view, size_t rhs)
    {
        /// No `default`: a new element must not be skipped silently.
        switch (element)
        {
            case Element::IsFloat:
                return compareNumbersAt(lhs_view.is_float, lhs, rhs_view.is_float, rhs);
            case Element::CounterResetHint:
            {
                /// Merging changes the counter hints, so a winner they decided would depend on the order of the merges. The gauge
                /// marker describes the sample itself and is never changed.
                const bool lhs_gauge = lhs_view.counterResetHintAt(lhs) == TimeSeriesHistogramCounterResetHint::Gauge;
                const bool rhs_gauge = rhs_view.counterResetHintAt(rhs) == TimeSeriesHistogramCounterResetHint::Gauge;
                return compareNumbers(lhs_gauge, rhs_gauge);
            }
            case Element::Schema:
                return compareNumbersAt(lhs_view.schema, lhs, rhs_view.schema, rhs);
            case Element::ZeroThreshold:
                return compareNumbersAt(lhs_view.zero_threshold, lhs, rhs_view.zero_threshold, rhs);
            case Element::Sum:
                return compareNumbersAt(lhs_view.sum, lhs, rhs_view.sum, rhs);
            case Element::CountInt:
                return compareNumbersAt(lhs_view.count_int, lhs, rhs_view.count_int, rhs);
            case Element::ZeroCountInt:
                return compareNumbersAt(lhs_view.zero_count_int, lhs, rhs_view.zero_count_int, rhs);
            case Element::CountFloat:
                return compareNumbersAt(lhs_view.count_float, lhs, rhs_view.count_float, rhs);
            case Element::ZeroCountFloat:
                return compareNumbersAt(lhs_view.zero_count_float, lhs, rhs_view.zero_count_float, rhs);
            case Element::PositiveInt:
                return compareBuckets(View::intPairsAt(lhs_view.positive_int, lhs), View::intPairsAt(rhs_view.positive_int, rhs));
            case Element::NegativeInt:
                return compareBuckets(View::intPairsAt(lhs_view.negative_int, lhs), View::intPairsAt(rhs_view.negative_int, rhs));
            case Element::PositiveFloat:
                return compareBuckets(View::floatPairsAt(lhs_view.positive_float, lhs), View::floatPairsAt(rhs_view.positive_float, rhs));
            case Element::NegativeFloat:
                return compareBuckets(View::floatPairsAt(lhs_view.negative_float, lhs), View::floatPairsAt(rhs_view.negative_float, rhs));
            case Element::CustomValues:
                return compareFloatArrays(View::floatsAt(lhs_view.custom_values, lhs), View::floatsAt(rhs_view.custom_values, rhs));
        }
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
    if (const int by_sum = compareNumbersAt(lhs_view.sum, lhs, rhs_view.sum, rhs))
        return by_sum;

    /// The remaining elements in the stored order. The counts and the sum are equal or were decided above.
    for (const auto element : TimeSeriesHistogramsColumns::getAll())
    {
        if (const int result = compareByElement(element, lhs_view, lhs, rhs_view, rhs))
            return result;
    }
    return 0;
}
}

}
