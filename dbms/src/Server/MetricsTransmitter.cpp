#include "MetricsTransmitter.h"

#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <daemon/BaseDaemon.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{

MetricsTransmitter::~MetricsTransmitter()
{
    try
    {
        {
            std::lock_guard<std::mutex> lock{mutex};
            quit = true;
        }

        cond.notify_one();

        thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MetricsTransmitter::run()
{
    const auto & config = context.getConfigRef();
    auto interval = config.getInt(config_name + ".interval", 60);

    const std::string thread_name = "MericsTrns " + std::to_string(interval) + "s";
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

    std::unique_lock<std::mutex> lock{mutex};

    while (true)
    {
        if (cond.wait_until(lock, get_next_time(interval), [this] { return quit; }))
            break;

        transmit(prev_counters);
    }
}


void MetricsTransmitter::transmit(std::vector<ProfileEvents::Count> & prev_counters)
{
    const auto & config = context.getConfigRef();
    auto async_metrics_values = async_metrics.getValues();

    GraphiteWriter::KeyValueVector<ssize_t> key_vals{};
    key_vals.reserve(ProfileEvents::end() + CurrentMetrics::end() + async_metrics_values.size());


    if (config.getBool(config_name + ".events", true))
    {
        for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::counters[i].load(std::memory_order_relaxed);
            const auto counter_increment = counter - prev_counters[i];
            prev_counters[i] = counter;

            std::string key{ProfileEvents::getDescription(static_cast<ProfileEvents::Event>(i))};
            key_vals.emplace_back(profile_events_path_prefix + key, counter_increment);
        }
    }

    if (config.getBool(config_name + ".metrics", true))
    {
        for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        {
            const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

            std::string key{CurrentMetrics::getDescription(static_cast<CurrentMetrics::Metric>(i))};
            key_vals.emplace_back(current_metrics_path_prefix + key, value);
        }
    }

    if (config.getBool(config_name + ".asynchronous_metrics", true))
    {
        for (const auto & name_value : async_metrics_values)
        {
            key_vals.emplace_back(asynchronous_metrics_path_prefix + name_value.first, name_value.second);
        }
    }

    if (key_vals.size())
        BaseDaemon::instance().writeToGraphite(key_vals, config_name);
}
}
