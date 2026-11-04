#include <Storages/TimeSeries/detectTimeSeriesHistogramResets.h>

#include <Common/assert_cast.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <algorithm>
#include <bit>


namespace DB
{
namespace
{
    using View = TimeSeriesHistogramsColumnsView;
    using Hint = TimeSeriesHistogramCounterResetHint;

    bool isStale(Float64 sum)
    {
        return std::bit_cast<UInt64>(sum) == 0x7ff0000000000002ULL;
    }

    /// A missing populated bucket is a reset. Missing empty buckets and newly appearing buckets are allowed.
    template <typename Count>
    bool bucketCountsDecreased(const View::Pairs<Count> & previous, const View::Pairs<Count> & current)
    {
        size_t current_index = 0;
        for (size_t i = 0; i < previous.size(); ++i)
        {
            while (current_index < current.size() && current.indexes[current_index] < previous.indexes[i])
                ++current_index;
            if (current_index == current.size() || current.indexes[current_index] != previous.indexes[i])
            {
                if (previous.counts[i] != 0)
                    return true;
            }
            else if (current.counts[current_index] < previous.counts[i])
                return true;
        }
        return false;
    }

    /// Matches the comparisons in Prometheus's histogram chunk appenders: schema and zero-threshold changes leave reset
    /// detection to the reader. Bucket counts can be compared directly only when those properties are unchanged.
    UInt8 detectReset(const View & columns, size_t previous, size_t current)
    {
        const bool is_float = columns.isFloatAt(current);
        if (is_float != columns.isFloatAt(previous) || columns.counter_reset_hint.getData()[previous] == Hint::Gauge)
            return Hint::Unknown;

        if (isStale(columns.sum.getData()[previous]))
            return Hint::Unknown;

        const bool count_decreased = is_float
            ? columns.count_float.getData()[current] < columns.count_float.getData()[previous]
            : columns.count_int.getData()[current] < columns.count_int.getData()[previous];
        if (count_decreased)
            return Hint::Reset;

        if (columns.schema.getData()[current] != columns.schema.getData()[previous]
            || columns.zero_threshold.getData()[current] != columns.zero_threshold.getData()[previous])
            return Hint::Unknown;

        /// Exponential histograms have empty custom bounds. The bounds are compared by value, as Prometheus's `CustomBucketBoundsMatch`
        /// does (NaN bounds are rejected on insert, so every bound compares equal to itself).
        const bool same_custom_bounds = std::ranges::equal(
            View::floatsAt(columns.custom_values, previous), View::floatsAt(columns.custom_values, current));
        if (!same_custom_bounds)
            return Hint::Reset;

        if (is_float)
        {
            if (columns.zero_count_float.getData()[current] < columns.zero_count_float.getData()[previous]
                || bucketCountsDecreased(View::floatPairsAt(columns.positive_float, previous), View::floatPairsAt(columns.positive_float, current))
                || bucketCountsDecreased(View::floatPairsAt(columns.negative_float, previous), View::floatPairsAt(columns.negative_float, current)))
                return Hint::Reset;
        }
        else
        {
            if (columns.zero_count_int.getData()[current] < columns.zero_count_int.getData()[previous]
                || bucketCountsDecreased(View::intPairsAt(columns.positive_int, previous), View::intPairsAt(columns.positive_int, current))
                || bucketCountsDecreased(View::intPairsAt(columns.negative_int, previous), View::intPairsAt(columns.negative_int, current)))
                return Hint::Reset;
        }
        return Hint::NoReset;
    }
}

void detectTimeSeriesHistogramResets(
    ColumnTuple & histograms,
    std::span<const size_t> original_indices,
    std::span<const size_t> series_offsets)
{
    const View columns{histograms};
    chassert(original_indices.size() == columns.size());
    auto & hints = assert_cast<ColumnUInt8 &>(histograms.getColumn(
        TimeSeriesHistogramsColumns::getTuplePosition(TimeSeriesHistogramsColumn::CounterResetHint))).getData();
    size_t begin = 0;
    for (const size_t end : series_offsets)
    {
        for (size_t i = begin; i < end; ++i)
        {
            /// Prometheus's chunk iterators return stale markers with only `Sum` set, leaving the hint unknown.
            /// This also applies to the first sample and overrides explicit reset and gauge hints.
            if (isStale(columns.sum.getData()[i]))
            {
                hints[i] = Hint::Unknown;
                continue;
            }
            if (i == begin || hints[i] == Hint::Gauge)
                continue;
            UInt8 hint = hints[i];
            if (original_indices[i] != original_indices[i - 1] + 1)
                Hint::clear(hint);
            if (hint != Hint::Reset)
                hint = detectReset(columns, i - 1, i);
            hints[i] = hint;
        }
        begin = end;
    }
}

}
