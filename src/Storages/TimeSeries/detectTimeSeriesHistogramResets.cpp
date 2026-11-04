#include <Storages/TimeSeries/detectTimeSeriesHistogramResets.h>

#include <Common/assert_cast.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

void detectTimeSeriesHistogramResets(ColumnTuple & histograms, std::span<const size_t> series_offsets)
{
    using Rule = TimeSeriesHistogramCounterResetRule;
    const TimeSeriesHistogramsColumnsView columns{histograms};
    auto & hints = assert_cast<ColumnUInt8 &>(histograms.getColumn(
        TimeSeriesHistogramsColumns::getTuplePosition(TimeSeriesHistogramsColumn::CounterResetHint))).getData();
    size_t begin = 0;
    for (const size_t end : series_offsets)
    {
        /// The predecessor of the first sample is in an earlier insert, which isn't consulted: the sample keeps the hint it was
        /// sent with, unless it is a stale marker.
        if ((begin < end) && Rule::isStaleMarker(columns.sum.getData()[begin]))
            hints[begin] = TimeSeriesHistogramCounterResetHint::Unknown;
        for (size_t i = begin + 1; i < end; ++i)
            hints[i] = Rule::hintAfterPredecessor(columns, i - 1, i);
        begin = end;
    }
}

}
