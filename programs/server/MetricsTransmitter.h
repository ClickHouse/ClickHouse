#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <optional>
#include <Core/Types.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>


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
  * - delta values of ProfileEvents;
  * - cumulative values of ProfileEvents;
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
    bool send_events_cumulative;
    bool send_metrics;
    bool send_asynchronous_metrics;

    bool quit = false;
    std::mutex mutex;
    std::condition_variable cond;
    std::optional<ThreadFromGlobalPool> thread;

    static constexpr auto profile_events_path_prefix = "ClickHouse.ProfileEvents.";
    static constexpr auto profile_events_cumulative_path_prefix = "ClickHouse.ProfileEventsCumulative.";
    static constexpr auto current_metrics_path_prefix = "ClickHouse.Metrics.";
    static constexpr auto asynchronous_metrics_path_prefix = "ClickHouse.AsynchronousMetrics.";
};

}
