#include <Storages/TimeSeries/detectTimeSeriesHistogramResets.h>

#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

void detectTimeSeriesHistogramResets(
    ColumnTuple & histograms,
    std::span<const size_t> series_offsets,
    std::span<const size_t> sorted_indices,
    std::span<const UInt8> duplicate_hints_differ)
{
    using Rule = TimeSeriesHistogramCounterResetRule;
    const TimeSeriesHistogramsColumnsMutableView columns{histograms};
    const TimeSeriesHistogramPayloadColumnsView payload{columns};
    auto & hints = columns.counter_reset_hint.getData();
    size_t begin = 0;
    for (const size_t end : series_offsets)
    {
        /// The predecessor of the first sample is in an earlier insert, which isn't consulted: the sample keeps the hint it was
        /// sent with, unless it is a stale marker or the dropped duplicates of it had other hints.
        if ((begin < end) && (Rule::isStaleMarker(payload.sum.getData()[begin]) || duplicate_hints_differ[sorted_indices[begin]]))
            hints[begin] = TimeSeriesHistogramCounterResetHint::Unknown;
        for (size_t i = begin + 1; i < end; ++i)
            hints[i] = Rule::hintAfterPredecessor(payload, i - 1, i);
        begin = end;
    }
}

}
