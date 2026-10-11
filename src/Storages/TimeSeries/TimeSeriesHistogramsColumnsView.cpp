#include <Storages/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <Core/Block.h>


namespace DB
{

TimeSeriesHistogramsColumnsView::TimeSeriesHistogramsColumnsView(const GetColumn & get_column)
    : is_float(get_column(TimeSeriesHistogramsColumn::IsFloat))
    , counter_reset_hint(get_column(TimeSeriesHistogramsColumn::CounterResetHint))
    , schema(get_column(TimeSeriesHistogramsColumn::Schema))
    , zero_threshold(get_column(TimeSeriesHistogramsColumn::ZeroThreshold))
    , sum(get_column(TimeSeriesHistogramsColumn::Sum))
    , positive_spans(get_column(TimeSeriesHistogramsColumn::PositiveSpans))
    , negative_spans(get_column(TimeSeriesHistogramsColumn::NegativeSpans))
    , custom_values(get_column(TimeSeriesHistogramsColumn::CustomValues))
    , count_int(get_column(TimeSeriesHistogramsColumn::CountInt))
    , zero_count_int(get_column(TimeSeriesHistogramsColumn::ZeroCountInt))
    , positive_values_int(get_column(TimeSeriesHistogramsColumn::PositiveValuesInt))
    , negative_values_int(get_column(TimeSeriesHistogramsColumn::NegativeValuesInt))
    , count_float(get_column(TimeSeriesHistogramsColumn::CountFloat))
    , zero_count_float(get_column(TimeSeriesHistogramsColumn::ZeroCountFloat))
    , positive_values_float(get_column(TimeSeriesHistogramsColumn::PositiveValuesFloat))
    , negative_values_float(get_column(TimeSeriesHistogramsColumn::NegativeValuesFloat))
{
}

TimeSeriesHistogramsColumnsView::TimeSeriesHistogramsColumnsView(const Block & block)
    : TimeSeriesHistogramsColumnsView([&](TimeSeriesHistogramsColumn column) -> const IColumn &
        {
            return *block.getByName(String{TimeSeriesHistogramsColumns::getName(column)}).column;
        })
{
}

TimeSeriesHistogramsColumnsView::TimeSeriesHistogramsColumnsView(const ColumnTuple & tuple)
    : TimeSeriesHistogramsColumnsView([&](TimeSeriesHistogramsColumn column) -> const IColumn &
        {
            return tuple.getColumn(static_cast<size_t>(column));
        })
{
}

}
