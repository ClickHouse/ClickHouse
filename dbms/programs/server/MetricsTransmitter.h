#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

class AsynchronousMetrics;


/** Automatically sends
  * - difference of ProfileEvents;
  * - values of CurrentMetrics;
  * - values of AsynchronousMetrics;
  *  to Graphite at beginning of every minute.
  */
class MetricsTransmitter
{
public:
    MetricsTransmitter(const Poco::Util::AbstractConfiguration & config, const std::string & config_name_, const AsynchronousMetrics & async_metrics_);
    ~MetricsTransmitter();

private:
    void run();
    void transmit(std::vector<ProfileEvents::Count> & prev_counters);

    const AsynchronousMetrics & async_metrics;

    std::string config_name;
    UInt32 interval_seconds;
    bool send_events;
    bool send_metrics;
    bool send_asynchronous_metrics;

    bool quit = false;
    std::mutex mutex;
    std::condition_variable cond;
    ThreadFromGlobalPool thread{&MetricsTransmitter::run, this};

    static inline constexpr auto profile_events_path_prefix = "ClickHouse.ProfileEvents.";
    static inline constexpr auto current_metrics_path_prefix = "ClickHouse.Metrics.";
    static inline constexpr auto asynchronous_metrics_path_prefix = "ClickHouse.AsynchronousMetrics.";
};

}
