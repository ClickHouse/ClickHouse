#include <base/getFQDNOrHostName.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/DateLUTImpl.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/BucketedMetricLog.h>
#include <Interpreters/Context.h>

#include <limits>


namespace DB
{

namespace Setting
{
    extern const SettingsBool system_metric_log_show_zero_values_in_histograms;
}

namespace
{

size_t numberOfMetrics()
{
    return ProfileEvents::end() + CurrentMetrics::end();
}

/// Global index of a metric: profile events come first, then current metrics.
std::string getMetricName(size_t global_index)
{
    if (global_index < ProfileEvents::end())
        return fmt::format("ProfileEvent_{}", ProfileEvents::getName(ProfileEvents::Event(global_index)));
    return fmt::format("CurrentMetric_{}", CurrentMetrics::getName(CurrentMetrics::Metric(global_index - ProfileEvents::end())));
}

}

ColumnsDescription BucketedMetricLogElement::getColumnsDescription()
{
    ColumnsDescription result;

    result.add({"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."});
    result.add({"event_date", std::make_shared<DataTypeDate>(), "Event date."});
    result.add({"event_time", std::make_shared<DataTypeDateTime>(), "Event time."});
    result.add({"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds resolution."});

    /// Enum16 values are the global metric indices.
    chassert(numberOfMetrics() <= static_cast<size_t>(std::numeric_limits<Int16>::max()));

    DataTypeEnum16::Values enum_values;
    enum_values.reserve(numberOfMetrics());
    for (size_t i = 0, end = numberOfMetrics(); i < end; ++i)
        enum_values.emplace_back(getMetricName(i), static_cast<Int16>(i));

    auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeEnum16>(std::move(enum_values)), std::make_shared<DataTypeInt64>());
    result.add({"metrics", std::move(map_type),
        "All profile events (as increments during the collection interval) and current metrics (as values at the moment of collection), "
        "mapped from the metric name to its value. Zero values are not stored; reading a missing key returns 0."});

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto labels_map_type = std::make_shared<DataTypeMap>(low_cardinality_string, low_cardinality_string);
    auto histogram_map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeUInt64>());

    result.add({"histograms.metric", std::make_shared<DataTypeArray>(low_cardinality_string), "Names of histogram families snapshotted in this row."});
    result.add({"histograms.labels", std::make_shared<DataTypeArray>(labels_map_type), "Per-entry label maps."});
    result.add({"histograms.histogram", std::make_shared<DataTypeArray>(histogram_map_type), "Per-entry cumulative bucket counts keyed by upper bound; +Inf is the final entry and equals count."});
    result.add({"histograms.count", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Per-entry total observation counts."});
    result.add({"histograms.sum", std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()), "Per-entry sums of observed values."});

    return result;
}


NamesAndAliases BucketedMetricLogElement::getNamesAndAliases()
{
    NamesAndAliases result;
    result.reserve(numberOfMetrics());

    const size_t num_profile_events = ProfileEvents::end();

    for (size_t i = 0, end = numberOfMetrics(); i < end; ++i)
    {
        auto name = getMetricName(i);
        DataTypePtr type = i < num_profile_events
            ? DataTypePtr(std::make_shared<DataTypeUInt64>())
            : DataTypePtr(std::make_shared<DataTypeInt64>());
        auto expression = fmt::format("metrics['{}']", name);
        result.emplace_back(std::move(name), std::move(type), std::move(expression));
    }

    return result;
}


void BucketedMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);

    auto & map_column = assert_cast<ColumnMap &>(*columns[column_idx++]);
    auto & keys = assert_cast<ColumnInt16 &>(map_column.getNestedData().getColumn(0));
    auto & values = assert_cast<ColumnInt64 &>(map_column.getNestedData().getColumn(1));

    const size_t num_profile_events = ProfileEvents::end();

    for (size_t i = 0, end = numberOfMetrics(); i < end; ++i)
    {
        const Int64 value = i < num_profile_events
            ? static_cast<Int64>(profile_events[i])
            : static_cast<Int64>(current_metrics[i - num_profile_events].toUnderType());

        /// Zero values are not stored: a lookup of a missing key in a Map returns the default value.
        if (value == 0)
            continue;

        keys.insertValue(static_cast<Int16>(i));
        values.insertValue(value);
    }

    map_column.getNestedColumn().getOffsets().push_back(keys.size());

    columns[column_idx++]->insert(histogram_metric);
    columns[column_idx++]->insert(histogram_labels);
    columns[column_idx++]->insert(histogram_histogram);
    columns[column_idx++]->insert(histogram_count);
    columns[column_idx++]->insert(histogram_sum);
}

void BucketedMetricLog::stepFunction(const std::chrono::system_clock::time_point current_time)
{
    std::lock_guard lock(previous_profile_events_mutex);

    const bool show_zero_values = getContext()->getSettingsRef()[Setting::system_metric_log_show_zero_values_in_histograms];

    add([&](BucketedMetricLogElement & element)
    {
        /// previous_profile_events is guarded by the mutex held above; thread-safety analysis cannot
        /// see the lock through this callback, so suppress the false positive on this access.
        MetricLogElement metric_log_element;
        collectMetricLogElement(
            metric_log_element, current_time, TSA_SUPPRESS_WARNING_FOR_WRITE(previous_profile_events), show_zero_values);

        element.event_time = metric_log_element.event_time;
        element.event_time_microseconds = metric_log_element.event_time_microseconds;
        element.profile_events = std::move(metric_log_element.profile_events);
        element.current_metrics = std::move(metric_log_element.current_metrics);
        element.histogram_metric = std::move(metric_log_element.histogram_metric);
        element.histogram_labels = std::move(metric_log_element.histogram_labels);
        element.histogram_histogram = std::move(metric_log_element.histogram_histogram);
        element.histogram_count = std::move(metric_log_element.histogram_count);
        element.histogram_sum = std::move(metric_log_element.histogram_sum);
    });
}

}
