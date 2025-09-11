#include <Processors/QueryPlan/CustomMetricLogViewStep.h>
#include <Common/HashTable/HashMap.h>
#include <Processors/IInflatingTransform.h>
#include <Common/PODArray.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/TransposedMetricLog.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>

#include <deque>
#include <unordered_map>

namespace DB
{

namespace
{
/// Transpose structure with metrics/events as rows to
/// to columnar view.
class CustomMetricLogViewTransform : public IInflatingTransform
{
    HashMap<StringRef, size_t> mapping;
    Names column_names;
public:
    static constexpr size_t SECONDS_IN_HOUR = 3600;
    String getName() const override { return "CustomMetricLogViewTransform"; }

    IColumnFilter filter;

    PODArray<UInt32> times;
    PODArray<UInt16> dates;
    std::vector<std::string> hostnames;
    Int32 current_hour = -1;
    Int64 max_second_in_hour = -1;
    Int64 min_second_in_hour = 3600;

    std::unordered_map<size_t, PODArray<Int64>> buffer;

    std::deque<Chunk> ready_hours;

    bool need_hostname = false;
    bool need_date = false;

    CustomMetricLogViewTransform(Block input_header, Block output_header)
        : IInflatingTransform(input_header, output_header)
    {
        size_t counter = 0;
        column_names.reserve(output_header.columns());

        /// Preserve memory for each column
        for (const auto & column : output_header)
        {
            column_names.push_back(column.name);
            const auto & column_name = column_names.back();
            if (column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX) || column_name.starts_with(TransposedMetricLog::CURRENT_METRIC_PREFIX))
            {
                mapping[column_name] = counter;
                buffer[counter].resize(SECONDS_IN_HOUR);
                counter++;
            }
            else if (column_name == TransposedMetricLog::HOSTNAME_NAME)
            {
                need_hostname = true;
            }
            else if (column_name == TransposedMetricLog::EVENT_DATE_NAME)
            {
                need_date = true;
            }
        }

        filter.resize_fill(SECONDS_IN_HOUR, static_cast<UInt8>(0));
        times.resize(SECONDS_IN_HOUR);

        if (need_date)
            dates.resize(SECONDS_IN_HOUR);
        if (need_hostname)
            hostnames.resize(SECONDS_IN_HOUR);
    }

    bool canGenerate() override
    {
        return !ready_hours.empty();
    }

    Chunk generate() override
    {
        auto result = std::move(ready_hours.front());
        ready_hours.pop_front();
        return result;
    }

    Chunk getRemaining() override
    {
        flushToChunk();

        if (ready_hours.empty())
            return Chunk();

        return generate();
    }

    void flushToChunk()
    {
        Chunk result;

        if (max_second_in_hour == -1)
            return;

        size_t rows_count = max_second_in_hour + 1 - min_second_in_hour;

        MutableColumns output_columns;
        output_columns.reserve(buffer.size() + need_date + need_hostname + 1);

        bool need_to_apply_filter = !memoryIsByte(filter.raw_data(), min_second_in_hour, max_second_in_hour + 1, 1);

        for (const auto & column_name : column_names)
        {
            if (column_name == TransposedMetricLog::EVENT_TIME_NAME)
            {
                if (need_to_apply_filter)
                {
                    auto column = ColumnDateTime::create();
                    column->reserve(rows_count);
                    for (size_t i = min_second_in_hour; i < static_cast<size_t>(max_second_in_hour + 1); ++i)
                    {
                        if (filter[i])
                            column->insertValue(times[i]);
                    }

                    rows_count = column->size();
                    output_columns.push_back(std::move(column));
                }
                else
                {
                    auto * start = times.begin() + min_second_in_hour;
                    auto * end = start + rows_count;
                    output_columns.push_back(ColumnDateTime::create(start, end));
                }
            }
            else if (column_name == TransposedMetricLog::EVENT_DATE_NAME)
            {
                if (need_to_apply_filter)
                {
                    auto column = ColumnDate::create();
                    column->reserve(rows_count);
                    for (size_t i = min_second_in_hour; i < static_cast<size_t>(max_second_in_hour + 1); ++i)
                        if (filter[i])
                            column->insertValue(dates[i]);
                    rows_count = column->size();
                    output_columns.push_back(std::move(column));
                }
                else
                {
                    auto * start = dates.begin() + min_second_in_hour;
                    auto * end = start + rows_count;
                    output_columns.push_back(ColumnDate::create(start, end));
                }
            }
            else if (column_name == TransposedMetricLog::HOSTNAME_NAME)
            {
                auto string_column = ColumnString::create();
                string_column->reserve(rows_count);

                for (size_t i = min_second_in_hour; i < static_cast<size_t>(max_second_in_hour + 1); ++i)
                {
                    if (filter[i])
                        string_column->insertData(hostnames[i].data(), hostnames[i].size());
                }
                rows_count = string_column->size();
                output_columns.push_back(std::move(string_column));
            }
            else if (column_name == TransposedMetricLog::EVENT_TIME_MICROSECONDS_NAME)
            {
                auto date_time_64_column = ColumnDateTime64::create(0, 6);
                date_time_64_column->reserve(rows_count);
                for (size_t i = min_second_in_hour; i < static_cast<size_t>(max_second_in_hour + 1); ++i)
                    if (filter[i])
                        date_time_64_column->insertValue(DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(times[i], 0, DecimalUtils::scaleMultiplier<Decimal64>(6)));
                rows_count = date_time_64_column->size();
                output_columns.push_back(std::move(date_time_64_column));

            }
            else if (column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX) || column_name.starts_with(TransposedMetricLog::CURRENT_METRIC_PREFIX))
            {
                bool is_event = column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX);
                auto & column = buffer[mapping.at(column_name)];
                if (need_to_apply_filter)
                {
                    MutableColumnPtr column_result;
                    if (is_event)
                        column_result = ColumnUInt64::create();
                    else
                        column_result = ColumnInt64::create();

                    column_result->reserve(rows_count);
                    for (size_t i = min_second_in_hour; i < static_cast<size_t>(max_second_in_hour + 1); ++i)
                    {
                        if (filter[i])
                        {
                            if (is_event)
                                static_cast<ColumnUInt64 *>(column_result.get())->insertValue(column[i]);
                            else
                                static_cast<ColumnInt64 *>(column_result.get())->insertValue(column[i]);
                        }
                    }
                    rows_count = column_result->size();
                    output_columns.push_back(std::move(column_result));
                }
                else
                {
                    auto * start = column.begin() + min_second_in_hour;
                    auto * end = start + rows_count;

                    if (is_event)
                        output_columns.push_back(ColumnUInt64::create(reinterpret_cast<uint64_t *>(start), reinterpret_cast<uint64_t *>(end)));
                    else
                        output_columns.push_back(ColumnInt64::create(start, end));
                }
                column.assign(SECONDS_IN_HOUR, static_cast<Int64>(0));
            }
        }

        filter.assign(SECONDS_IN_HOUR, static_cast<UInt8>(0));

        result.setColumns(std::move(output_columns), rows_count);
        ready_hours.emplace_back(std::move(result));
    }

    void consume(Chunk chunk) override
    {
        size_t rows_count = chunk.getNumRows();

        const auto & columns = chunk.getColumns();
        const auto & event_time_column = checkAndGetColumn<ColumnDateTime>(*columns[TransposedMetricLog::EVENT_TIME_POSITION]);
        const auto & value_column = checkAndGetColumn<ColumnInt64>(*columns[TransposedMetricLog::VALUE_POSITION]);
        const auto & metric_column = checkAndGetColumn<ColumnLowCardinality>(*columns[TransposedMetricLog::METRIC_POSITION]);
        const auto & date_column = checkAndGetColumn<ColumnDate>(*columns[TransposedMetricLog::EVENT_DATE_POSITION]);
        const auto & hostname_column = checkAndGetColumn<ColumnLowCardinality>(*columns[TransposedMetricLog::HOSTNAME_POSITION]);
        const auto & hour_column = checkAndGetColumn<ColumnDateTime>(*columns[TransposedMetricLog::EVENT_TIME_HOUR_POSITION]);

        if (rows_count && current_hour == -1)
        {
            current_hour = hour_column.getInt(0);
        }

        for (size_t i = 0; i < rows_count; ++i)
        {
            Int32 hour = hour_column.getInt(i);
            if (hour != current_hour)
            {
                flushToChunk();

                current_hour = hour;
                max_second_in_hour = -1;
                min_second_in_hour = 3600;
            }

            auto time = event_time_column.getInt(i);

            auto second_in_hour = time - hour;
            max_second_in_hour = std::max<Int64>(second_in_hour, max_second_in_hour);
            min_second_in_hour = std::min<Int64>(second_in_hour, min_second_in_hour);
            times[second_in_hour] = time;
            filter[second_in_hour] = 1;

            if (need_date)
                dates[second_in_hour] = date_column.getUInt(i);

            if (need_hostname)
                hostnames[second_in_hour] = hostname_column.getDataAt(i).toString();

            StringRef metric_name = metric_column.getDataAt(i);
            auto * it = mapping.find(metric_name);
            if (it == mapping.end())
                continue;

            size_t event_index = it->value.second;

            Int64 value = value_column.getInt(i);
            buffer[event_index][second_in_hour] = value;
        }
    }
};

}

CustomMetricLogViewStep::CustomMetricLogViewStep(
    Block input_header_, Block output_header_)
     : ITransformingStep(
         input_header_, output_header_,
         ITransformingStep::Traits
         {
            .data_stream_traits = ITransformingStep::DataStreamTraits{.returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = false},
            .transform_traits = ITransformingStep::TransformTraits{.preserves_number_of_rows = false}
         })
{
    sort_description.emplace_back(SortColumnDescription("event_time"));
}

void CustomMetricLogViewStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<CustomMetricLogViewTransform>(input_headers[0], *output_header));
}

const SortDescription & CustomMetricLogViewStep::getSortDescription() const
{
    return sort_description;
}

}
