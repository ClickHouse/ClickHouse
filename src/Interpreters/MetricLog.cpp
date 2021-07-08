#include <Interpreters/MetricLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

Block MetricLogElement::createBlock()
{
    ColumnsWithTypeAndName columns_with_type_and_name;

    columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDate>(),           "event_date");
    columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDateTime>(),       "event_time");
    columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDateTime64>(6),    "event_time_microseconds");
    columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(),         "milliseconds");

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        std::string name;
        name += "ProfileEvent_";
        name += ProfileEvents::getName(ProfileEvents::Event(i));
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), std::move(name));
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        std::string name;
        name += "CurrentMetric_";
        name += CurrentMetrics::getName(CurrentMetrics::Metric(i));
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeInt64>(), std::move(name));
    }

    return Block(columns_with_type_and_name);
}


void MetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(milliseconds);

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        columns[column_idx++]->insert(profile_events[i]);

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        columns[column_idx++]->insert(current_metrics[i]);
}


void MetricLog::startCollectMetric(size_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;
    is_shutdown_metric_thread = false;
    metric_flush_thread = ThreadFromGlobalPool([this] { metricThreadFunction(); });
}


void MetricLog::stopCollectMetric()
{
    bool old_val = false;
    if (!is_shutdown_metric_thread.compare_exchange_strong(old_val, true))
        return;
    metric_flush_thread.join();
}


void MetricLog::shutdown()
{
    stopCollectMetric();
    stopFlushThread();
}


inline UInt64 time_in_milliseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}


void MetricLog::metricThreadFunction()
{
    auto desired_timepoint = std::chrono::system_clock::now();

    /// For differentiation of ProfileEvents counters.
    std::vector<ProfileEvents::Count> prev_profile_events(ProfileEvents::end());

    while (!is_shutdown_metric_thread)
    {
        try
        {
            const auto current_time = std::chrono::system_clock::now();

            MetricLogElement elem;
            elem.event_time = std::chrono::system_clock::to_time_t(current_time);
            elem.event_time_microseconds = time_in_microseconds(current_time);
            elem.milliseconds = time_in_milliseconds(current_time) - time_in_seconds(current_time) * 1000;

            elem.profile_events.resize(ProfileEvents::end());
            for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
            {
                const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
                auto & old_value = prev_profile_events[i];
                elem.profile_events[i] = new_value - old_value;
                old_value = new_value;
            }

            elem.current_metrics.resize(CurrentMetrics::end());
            for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
            {
                elem.current_metrics[i] = CurrentMetrics::values[i];
            }

            this->add(elem);

            /// We will record current time into table but align it to regular time intervals to avoid time drift.
            /// We may drop some time points if the server is overloaded and recording took too much time.
            while (desired_timepoint <= current_time)
                desired_timepoint += std::chrono::milliseconds(collect_interval_milliseconds);

            std::this_thread::sleep_until(desired_timepoint);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
