#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/MetricLog.h>
#include <base/getFQDNOrHostName.h>
#include <Common/ProfileEvents.h>
#include <Common/DateLUTImpl.h>
#include <Common/ThreadPool.h>


namespace DB
{

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
        const auto * comment = ProfileEvents::getDocumentation(ProfileEvents::Event(i));
        result.add({std::move(name), std::make_shared<DataTypeUInt64>(), comment});
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        auto name = fmt::format("CurrentMetric_{}", CurrentMetrics::getName(CurrentMetrics::Metric(i)));
        const auto * comment = CurrentMetrics::getDocumentation(CurrentMetrics::Metric(i));
        result.add({std::move(name), std::make_shared<DataTypeInt64>(), comment});
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

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        columns[column_idx++]->insert(profile_events[i]);

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        columns[column_idx++]->insert(current_metrics[i].toUnderType());
}

void MetricLog::stepFunction(const std::chrono::system_clock::time_point current_time)
{
    /// Static lazy initialization to avoid polluting the header with implementation details
    /// For differentiation of ProfileEvents counters.
    static std::vector<ProfileEvents::Count> prev_profile_events(ProfileEvents::end());

    MetricLogElement elem;
    elem.event_time = std::chrono::system_clock::to_time_t(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);

    elem.profile_events.resize(ProfileEvents::end());
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        auto & old_value = prev_profile_events[i];
        auto diff = new_value - old_value;
        elem.profile_events[i] = diff;
        ProfileEvents::global_counters.updateWindow(i, diff);
        old_value = new_value;
    }
    ProfileEvents::global_counters.allWindowsUpdated();

    elem.current_metrics.resize(CurrentMetrics::end());
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        elem.current_metrics[i] = CurrentMetrics::values[i];
    }

    this->add(std::move(elem));
}

}
