#include <Storages/TimeSeries/detectTimeSeriesHistogramResets.h>

#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>


namespace DB
{

namespace Rule = TimeSeriesHistogramCounterResetRule;

void detectTimeSeriesHistogramResets(ColumnTuple & histograms, std::span<const size_t> offsets)
{
    const TimeSeriesHistogramsColumnsMutableView columns{histograms};
    const TimeSeriesHistogramPayloadColumnsView payload{columns};
    size_t begin = 0;
    for (const size_t end : offsets)
    {
        for (size_t i = begin; i < end; ++i)
            columns.setCounterResetHintAt(i, Rule::detectReset(payload, (i == begin) ? Rule::NO_PREVIOUS : i - 1, i));
        begin = end;
    }
}

void setFirstTimeSeriesHistogramHints(ColumnTuple & histograms, std::span<const size_t> offsets)
{
    const TimeSeriesHistogramsColumnsMutableView columns{histograms};
    const TimeSeriesHistogramPayloadColumnsView payload{columns};
    size_t begin = 0;
    for (const size_t end : offsets)
    {
        if (begin < end)
            columns.setCounterResetHintAt(begin, Rule::detectReset(payload, Rule::NO_PREVIOUS, begin));
        begin = end;
    }
}

}
