#include <Storages/TimeSeries/detectTimeSeriesHistogramResets.h>

#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

void detectTimeSeriesHistogramResets(ColumnTuple & histograms, std::span<const size_t> series_offsets)
{
    using Rule = TimeSeriesHistogramCounterResetRule;
    const TimeSeriesHistogramsColumnsMutableView columns{histograms};
    const TimeSeriesHistogramPayloadColumnsView payload{columns};
    auto & hints = columns.counter_reset_hint.getData();
    size_t begin = 0;
    for (const size_t end : series_offsets)
    {
        /// The predecessor of the first sample is in an earlier insert, which isn't consulted: the sample keeps the hint it was
        /// sent with, unless it is a stale marker.
        if ((begin < end) && Rule::isStaleMarker(payload.sum.getData()[begin]))
            hints[begin] = TimeSeriesHistogramCounterResetHint::Unknown;
        /// The predecessor within one INSERT is the one the sender saw, so an explicit reset is trusted: a reset after a schema change
        /// can't be told from the data.
        for (size_t i = begin + 1; i < end; ++i)
            hints[i] = Rule::hintAfterPredecessor(payload, i - 1, i, /* preserve_explicit_reset = */ true);
        begin = end;
    }
}

}
