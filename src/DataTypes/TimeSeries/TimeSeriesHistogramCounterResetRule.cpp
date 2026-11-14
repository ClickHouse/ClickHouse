#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>

#include <algorithm>
#include <bit>


namespace DB
{

namespace
{
    using View = TimeSeriesHistogramPayloadColumnsView;
    using Hint = TimeSeriesHistogramCounterResetHint;

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
}


bool TimeSeriesHistogramCounterResetRule::isStaleMarker(Float64 sum)
{
    return std::bit_cast<UInt64>(sum) == 0x7ff0000000000002ULL;
}

UInt8 TimeSeriesHistogramCounterResetRule::hintAfterPredecessor(const View & view, size_t previous, size_t current)
{
    /// A stale marker carries only `sum`, so its counts can't be compared with the predecessor's.
    if (isStaleMarker(view.sum.getData()[current]))
        return Hint::Unknown;
    /// A gauge marker describes the sample itself, not its relation to the predecessor. A counter hint the sample was sent with is
    /// about the predecessor its sender saw, which this one need not be, so it is detected from the data instead.
    const UInt8 hint = view.counter_reset_hint.getData()[current];
    if (hint == Hint::Gauge)
        return hint;
    return detect(view, previous, current);
}

UInt8 TimeSeriesHistogramCounterResetRule::detect(const View & columns, size_t previous, size_t current)
{
    const bool is_float = columns.isFloatAt(current);
    if (is_float != columns.isFloatAt(previous) || columns.counter_reset_hint.getData()[previous] == Hint::Gauge)
        return Hint::Unknown;

    if (isStaleMarker(columns.sum.getData()[previous]))
        return Hint::Unknown;

    const bool count_decreased = is_float
        ? columns.count_float.getData()[current] < columns.count_float.getData()[previous]
        : columns.count_int.getData()[current] < columns.count_int.getData()[previous];
    if (count_decreased)
        return Hint::Reset;

    /// Bucket counts are comparable only under the same schema and zero threshold; otherwise the detection is left to the reader.
    if (columns.schema.getData()[current] != columns.schema.getData()[previous]
        || columns.zero_threshold.getData()[current] != columns.zero_threshold.getData()[previous])
        return Hint::Unknown;

    /// Comparing the bounds by value is sound because NaN bounds are rejected on insert.
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
