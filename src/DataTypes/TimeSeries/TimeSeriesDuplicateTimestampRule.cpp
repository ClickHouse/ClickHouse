#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>

#include <DataTypes/TimeSeries/PrometheusStaleMarker.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/DecomposedFloat.h>

#include <algorithm>
#include <concepts>
#include <span>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramPayloadColumnsView;
    using Element = TimeSeriesHistogramsColumn;

    template <std::integral T>
    int compareIntegers(T lhs, T rhs)
    {
        if (lhs != rhs)
            return (lhs < rhs) ? -1 : 1;
        return 0;
    }

    template <std::integral T>
    int compareIntegersAt(const ColumnVector<T> & lhs_column, size_t lhs, const ColumnVector<T> & rhs_column, size_t rhs)
    {
        return compareIntegers(lhs_column.getData()[lhs], rhs_column.getData()[rhs]);
    }

    int compareFloatsAt(const ColumnFloat64 & lhs_column, size_t lhs, const ColumnFloat64 & rhs_column, size_t rhs)
    {
        return TimeSeriesDuplicateTimestampRule::compareFloats(lhs_column.getData()[lhs], rhs_column.getData()[rhs]);
    }

    /// The order of `compareFloats` between a float and an integer, which stands for the float of the same value (0 for `0.0`).
    /// Exact: the integer isn't rounded to `Float64`.
    int compareFloatWithInteger(Float64 float_value, Int64 integer)
    {
        /// `DecomposedFloat::compare` orders a NaN as an infinity of its sign; `compareFloats` puts it below any number.
        if (isNaN(float_value))
            return -1;
        /// `DecomposedFloat::compare` doesn't tell `-0.0` from 0, and 0 is exactly `0.0`.
        if (integer == 0)
            return TimeSeriesDuplicateTimestampRule::compareFloats(float_value, 0.0);
        return DecomposedFloat<Float64>(float_value).compare(integer);
    }

    /// The lexicographic order over arrays: by the first differing element, a prefix first. `compare_elements(i)` compares
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
        return compareIntegers(lhs_size, rhs_size);
    }

    /// The order over the buckets, arrays of tuples: a pair by the index, then by the count with `compare_counts`.
    template <typename Count, typename CompareCounts>
    int compareBuckets(const View::Pairs<Count> & lhs, const View::Pairs<Count> & rhs, const CompareCounts & compare_counts)
    {
        return compareArrays(lhs.size(), rhs.size(), [&](size_t i)
        {
            if (const int by_index = compareIntegers(lhs.indexes[i], rhs.indexes[i]))
                return by_index;
            return compare_counts(lhs.counts[i], rhs.counts[i]);
        });
    }

    int compareIntBuckets(const View::IntPairs & lhs, const View::IntPairs & rhs)
    {
        return compareBuckets(lhs, rhs, [](Int64 lhs_count, Int64 rhs_count) { return compareIntegers(lhs_count, rhs_count); });
    }

    int compareFloatBuckets(const View::FloatPairs & lhs, const View::FloatPairs & rhs)
    {
        return compareBuckets(lhs, rhs, [](Float64 lhs_count, Float64 rhs_count)
        {
            return TimeSeriesDuplicateTimestampRule::compareFloats(lhs_count, rhs_count);
        });
    }

    int compareFloatArrays(std::span<const Float64> lhs, std::span<const Float64> rhs)
    {
        return compareArrays(lhs.size(), rhs.size(), [&](size_t i)
        {
            return TimeSeriesDuplicateTimestampRule::compareFloats(lhs[i], rhs[i]);
        });
    }

    /// The second key: the total count, compared exactly even across flavours without rounding an integer to `Float64`.
    int compareCounts(const View & lhs_view, size_t lhs, const View & rhs_view, size_t rhs)
    {
        const bool lhs_float = lhs_view.isFloatAt(lhs);
        const bool rhs_float = rhs_view.isFloatAt(rhs);
        if (lhs_float != rhs_float)
        {
            const Float64 float_count = (lhs_float ? lhs_view : rhs_view).count_float.getData()[lhs_float ? lhs : rhs];
            const Int64 int_count = (lhs_float ? rhs_view : lhs_view).count_int.getData()[lhs_float ? rhs : lhs];
            const int result = compareFloatWithInteger(float_count, int_count);
            return lhs_float ? result : -result;
        }
        if (lhs_float)
            return compareFloatsAt(lhs_view.count_float, lhs, rhs_view.count_float, rhs);
        return compareIntegersAt(lhs_view.count_int, lhs, rhs_view.count_int, rhs);
    }

    /// The samples `lhs` of `lhs_view` and `rhs` of `rhs_view` by `element`: floats with `compareFloats`, the counter reset hints by
    /// the gauge marker only.
    int compareByElement(Element element, const View & lhs_view, size_t lhs, const View & rhs_view, size_t rhs)
    {
        /// No `default`: a new element must not be skipped silently.
        switch (element)
        {
            case Element::IsFloat:
                return compareIntegersAt(lhs_view.is_float, lhs, rhs_view.is_float, rhs);
            case Element::CounterResetHint:
            {
                /// Merging changes the counter hints, so a winner they decided would depend on the order of the merges. The gauge
                /// marker describes the sample itself and is never changed.
                const bool lhs_gauge = lhs_view.counterResetHintAt(lhs) == TimeSeriesHistogramCounterResetHint::Gauge;
                const bool rhs_gauge = rhs_view.counterResetHintAt(rhs) == TimeSeriesHistogramCounterResetHint::Gauge;
                return compareIntegers(lhs_gauge, rhs_gauge);
            }
            case Element::Schema:
                return compareIntegersAt(lhs_view.schema, lhs, rhs_view.schema, rhs);
            case Element::ZeroThreshold:
                return compareFloatsAt(lhs_view.zero_threshold, lhs, rhs_view.zero_threshold, rhs);
            case Element::Sum:
                return compareFloatsAt(lhs_view.sum, lhs, rhs_view.sum, rhs);
            case Element::CountInt:
                return compareIntegersAt(lhs_view.count_int, lhs, rhs_view.count_int, rhs);
            case Element::ZeroCountInt:
                return compareIntegersAt(lhs_view.zero_count_int, lhs, rhs_view.zero_count_int, rhs);
            case Element::CountFloat:
                return compareFloatsAt(lhs_view.count_float, lhs, rhs_view.count_float, rhs);
            case Element::ZeroCountFloat:
                return compareFloatsAt(lhs_view.zero_count_float, lhs, rhs_view.zero_count_float, rhs);
            case Element::PositiveInt:
                return compareIntBuckets(View::intPairsAt(lhs_view.positive_int, lhs), View::intPairsAt(rhs_view.positive_int, rhs));
            case Element::NegativeInt:
                return compareIntBuckets(View::intPairsAt(lhs_view.negative_int, lhs), View::intPairsAt(rhs_view.negative_int, rhs));
            case Element::PositiveFloat:
                return compareFloatBuckets(View::floatPairsAt(lhs_view.positive_float, lhs), View::floatPairsAt(rhs_view.positive_float, rhs));
            case Element::NegativeFloat:
                return compareFloatBuckets(View::floatPairsAt(lhs_view.negative_float, lhs), View::floatPairsAt(rhs_view.negative_float, rhs));
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
    if (const int by_sum = compareFloatsAt(lhs_view.sum, lhs, rhs_view.sum, rhs))
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
