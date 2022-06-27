#include "MetricsTransmitter.h"

#include <Interpreters/AsynchronousMetrics.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <Daemon/BaseDaemon.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{

MetricsTransmitter::MetricsTransmitter(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_name_, const AsynchronousMetrics & async_metrics_)
    : async_metrics(async_metrics_), config_name(config_name_)
{
    interval_seconds = config.getInt(config_name + ".interval", 60);
    send_events = config.getBool(config_name + ".events", true);
    send_events_cumulative = config.getBool(config_name + ".events_cumulative", false);
    send_metrics = config.getBool(config_name + ".metrics", true);
    send_asynchronous_metrics = config.getBool(config_name + ".asynchronous_metrics", true);

    thread = ThreadFromGlobalPool{&MetricsTransmitter::run, this};
}


MetricsTransmitter::~MetricsTransmitter()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        cond.notify_one();

        thread->join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MetricsTransmitter::run()
{
    const std::string thread_name = "MetrTx" + std::to_string(interval_seconds);
    setThreadName(thread_name.c_str());

    const auto get_next_time = [](size_t seconds)
    {
        /// To avoid time drift and transmit values exactly each interval:
        ///  next time aligned to system seconds
        /// (60s -> every minute at 00 seconds, 5s -> every minute:[00, 05, 15 ... 55]s, 3600 -> every hour:00:00
        return std::chrono::system_clock::time_point(
            (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()) / seconds) * seconds
            + std::chrono::seconds(seconds));
    };

    std::vector<ProfileEvents::Count> prev_counters(ProfileEvents::end());

    std::unique_lock lock{mutex};

    while (true)
    {
        if (cond.wait_until(lock, get_next_time(interval_seconds), [this] { return quit; }))
            break;

        transmit(prev_counters);
    }
}


void MetricsTransmitter::transmit(std::vector<ProfileEvents::Count> & prev_counters)
{
    auto async_metrics_values = async_metrics.getValues();

    GraphiteWriter::KeyValueVector<ssize_t> key_vals{};
    key_vals.reserve(ProfileEvents::end() + CurrentMetrics::end() + async_metrics_values.size());

    if (send_events)
    {
        for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
            const auto counter_increment = counter - prev_counters[i];
            prev_counters[i] = counter;

            std::string key{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
            key_vals.emplace_back(profile_events_path_prefix + key, counter_increment);
        }
    }

    if (send_events_cumulative)
    {
        for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
            std::string key{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
            key_vals.emplace_back(profile_events_cumulative_path_prefix + key, counter);
        }
    }

    if (send_metrics)
    {
        for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        {
            const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

            std::string key{CurrentMetrics::getName(static_cast<CurrentMetrics::Metric>(i))};
            key_vals.emplace_back(current_metrics_path_prefix + key, value);
        }
    }

    if (send_asynchronous_metrics)
    {
        for (const auto & name_value : async_metrics_values)
        {
            key_vals.emplace_back(asynchronous_metrics_path_prefix + name_value.first, name_value.second);
        }
    }

    if (!key_vals.empty())
        BaseDaemon::instance().writeToGraphite(key_vals, config_name);
}

}
