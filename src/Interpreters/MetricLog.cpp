#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/HistogramMetrics.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
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

ColumnsDescription MetricLogElement::getColumnsDescription()
{
    ColumnsDescription result;

    result.add({"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."});
    result.add({"event_date", std::make_shared<DataTypeDate>(), "Event date."});
    result.add({"event_time", std::make_shared<DataTypeDateTime>(), "Event time."});
    result.add({"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds resolution."});

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        auto name = fmt::format("ProfileEvent_{}", ProfileEvents::getName(ProfileEvents::Event(i)));
        std::string_view comment = ProfileEvents::getDocumentation(ProfileEvents::Event(i));
        result.add({std::move(name), std::make_shared<DataTypeUInt64>(), std::string(comment)});
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        auto name = fmt::format("CurrentMetric_{}", CurrentMetrics::getName(CurrentMetrics::Metric(i)));
        std::string_view comment = CurrentMetrics::getDocumentation(CurrentMetrics::Metric(i));
        result.add({std::move(name), std::make_shared<DataTypeInt64>(), std::string(comment)});
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


void MetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        columns[column_idx++]->insert(profile_events[i]);

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        columns[column_idx++]->insert(current_metrics[i].toUnderType());

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
        const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
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
