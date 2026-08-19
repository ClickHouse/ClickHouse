#include <base/getFQDNOrHostName.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/DateLUTImpl.h>
#include <Common/HistogramMetrics.h>
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
#include <Interpreters/Context.h>
#include <Interpreters/MetricLog.h>

#include <limits>


namespace DB
{

namespace Setting
{
    extern const SettingsBool system_metric_log_show_zero_values_in_histograms;
}

namespace
{

/// Profile events and current metrics are stored in a fixed number of Map columns (buckets)
/// with an Enum16 of the metric name as a key. This way the table has a fixed and small number
/// of columns, while reading a single metric requires to read only a small fraction of the data.
constexpr size_t NUM_METRIC_BUCKETS = 128;

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

/// Metrics with global indices in [bucketBegin(b), bucketBegin(b + 1)) belong to bucket b.
/// Contiguous ranges are used, so metrics that are declared (and typically queried) together
/// end up in the same bucket.
size_t bucketBegin(size_t bucket)
{
    return bucket * numberOfMetrics() / NUM_METRIC_BUCKETS;
}

}

ColumnsDescription MetricLogElement::getColumnsDescription()
{
    ColumnsDescription result;

    result.add({"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."});
    result.add({"event_date", std::make_shared<DataTypeDate>(), "Event date."});
    result.add({"event_time", std::make_shared<DataTypeDateTime>(), "Event time."});
    result.add({"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds resolution."});

    /// Enum16 values are the global metric indices.
    chassert(numberOfMetrics() <= static_cast<size_t>(std::numeric_limits<Int16>::max()));

    for (size_t bucket = 0; bucket < NUM_METRIC_BUCKETS; ++bucket)
    {
        DataTypeEnum16::Values enum_values;
        const size_t begin = bucketBegin(bucket);
        const size_t end = bucketBegin(bucket + 1);
        enum_values.reserve(end - begin);
        for (size_t i = begin; i < end; ++i)
            enum_values.emplace_back(getMetricName(i), static_cast<Int16>(i));

        auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeEnum16>(std::move(enum_values)), std::make_shared<DataTypeInt64>());
        result.add({fmt::format("metrics_{}", bucket), std::move(map_type),
            "A bucket of profile events (as increments during the collection interval) and current metrics (as values at the moment of collection), "
            "mapped from the metric name to its value. Zero values are not stored; reading a missing key returns 0."});
    }

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


NamesAndAliases MetricLogElement::getNamesAndAliases()
{
    NamesAndAliases result;
    result.reserve(numberOfMetrics());

    const size_t num_profile_events = ProfileEvents::end();

    for (size_t bucket = 0; bucket < NUM_METRIC_BUCKETS; ++bucket)
    {
        const size_t begin = bucketBegin(bucket);
        const size_t end = bucketBegin(bucket + 1);
        for (size_t i = begin; i < end; ++i)
        {
            auto name = getMetricName(i);
            DataTypePtr type = i < num_profile_events
                ? DataTypePtr(std::make_shared<DataTypeUInt64>())
                : DataTypePtr(std::make_shared<DataTypeInt64>());
            auto expression = fmt::format("metrics_{}['{}']", bucket, name);
            result.emplace_back(std::move(name), std::move(type), std::move(expression));
        }
    }

    return result;
}


void MetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);

    const size_t num_profile_events = ProfileEvents::end();

    for (size_t bucket = 0; bucket < NUM_METRIC_BUCKETS; ++bucket)
    {
        auto & map_column = assert_cast<ColumnMap &>(*columns[column_idx++]);
        auto & keys = assert_cast<ColumnInt16 &>(map_column.getNestedData().getColumn(0));
        auto & values = assert_cast<ColumnInt64 &>(map_column.getNestedData().getColumn(1));

        const size_t begin = bucketBegin(bucket);
        const size_t end = bucketBegin(bucket + 1);
        for (size_t i = begin; i < end; ++i)
        {
            const Int64 value = i < num_profile_events
                ? static_cast<Int64>(profile_events[i])
                : current_metrics[i - num_profile_events];

            /// Zero values are not stored: a lookup of a missing key in a Map returns the default value.
            if (value == 0)
                continue;

            keys.insertValue(static_cast<Int16>(i));
            values.insertValue(value);
        }

        map_column.getNestedColumn().getOffsets().push_back(keys.size());
    }

    columns[column_idx++]->insert(histogram_metric);
    columns[column_idx++]->insert(histogram_labels);
    columns[column_idx++]->insert(histogram_histogram);
    columns[column_idx++]->insert(histogram_count);
    columns[column_idx++]->insert(histogram_sum);
}

void MetricLog::stepFunction(const std::chrono::system_clock::time_point current_time)
{
    std::lock_guard lock(previous_profile_events_mutex);

    MetricLogElement elem;
    elem.event_time = std::chrono::system_clock::to_time_t(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);

    elem.profile_events.resize(ProfileEvents::end());
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count new_value = ProfileEvents::global_counters[i];
        auto & old_value = previous_profile_events[i];

        /// Profile event counters are supposed to be monotonic. However, at least the `NetworkReceiveBytes` can be inaccurate.
        /// So, since in the future the counter should always have a bigger value than in the past, we skip this event.
        /// It can be reproduced with the following integration tests:
        /// - test_hedged_requests/test.py::test_receive_timeout2
        /// - test_secure_socket::test
        if (new_value < old_value)
            continue;

        elem.profile_events[i] = new_value - old_value;
        old_value = new_value;
    }

    elem.current_metrics.resize(CurrentMetrics::end());
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        elem.current_metrics[i] = CurrentMetrics::values[i];
    }

    const bool show_zero_values = getContext()->getSettingsRef()[Setting::system_metric_log_show_zero_values_in_histograms];

    HistogramMetrics::Factory::instance().forEachFamily([&](const HistogramMetrics::MetricFamily & family)
    {
        const auto & buckets = family.getBuckets();
        const auto & label_names = family.getLabels();
        const auto & metric_name = family.getName();

        family.forEachMetric([&](const HistogramMetrics::LabelValues & label_values, const HistogramMetrics::Metric & metric)
        {
            Map labels;
            labels.reserve(label_values.size());
            for (size_t i = 0; i < label_values.size(); ++i)
                labels.push_back(Tuple{label_names[i], label_values[i]});

            Map histogram_map;
            histogram_map.reserve(buckets.size() + 1);
            UInt64 cumulative = 0;
            for (size_t i = 0; i < buckets.size() + 1; ++i)
            {
                const UInt64 counter = metric.getCounter(i);
                const bool is_inf_bucket = (i == buckets.size());
                if (counter == 0 && !is_inf_bucket && !show_zero_values)
                    continue;
                cumulative += counter;
                Float64 bound = is_inf_bucket ? std::numeric_limits<Float64>::infinity() : buckets[i];
                histogram_map.push_back(Tuple{bound, cumulative});
            }

            if (cumulative == 0 && !show_zero_values)
                return;

            elem.histogram_metric.push_back(metric_name);
            elem.histogram_labels.push_back(std::move(labels));
            elem.histogram_histogram.push_back(std::move(histogram_map));
            elem.histogram_count.push_back(cumulative);
            elem.histogram_sum.push_back(metric.getSum());
        });
    });

    add(std::move(elem));
}

}
