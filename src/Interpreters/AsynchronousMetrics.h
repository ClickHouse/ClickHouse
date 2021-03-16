#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <string>
#include <Common/ThreadPool.h>
#include <Common/MemoryStatisticsOS.h>


namespace DB
{

class Context;
class ProtocolServerAdapter;

using AsynchronousMetricValue = double;
using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;


/** Periodically (by default, each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  */
class AsynchronousMetrics
{
public:
#if defined(ARCADIA_BUILD)
    /// This constructor needs only to provide backward compatibility with some other projects (hello, Arcadia).
    /// Never use this in the ClickHouse codebase.
    AsynchronousMetrics(
        Context & global_context_,
        int update_period_seconds = 60)
        : global_context(global_context_)
        , update_period(update_period_seconds)
    {
    }
#endif

    /// The default value of update_period_seconds is for ClickHouse-over-YT
    /// in Arcadia -- it uses its own server implementation that also uses these
    /// metrics.
    AsynchronousMetrics(
        Context & global_context_,
        int update_period_seconds,
        std::shared_ptr<std::vector<ProtocolServerAdapter>> servers_to_start_before_tables_,
        std::shared_ptr<std::vector<ProtocolServerAdapter>> servers_)
        : global_context(global_context_)
        , update_period(update_period_seconds)
        , servers_to_start_before_tables(servers_to_start_before_tables_)
        , servers(servers_)
    {
    }

    ~AsynchronousMetrics();

    /// Separate method allows to initialize the `servers` variable beforehand.
    void start()
    {
        thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
    }

    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

private:
    Context & global_context;
    const std::chrono::seconds update_period;
    std::shared_ptr<std::vector<ProtocolServerAdapter>> servers_to_start_before_tables{nullptr};
    std::shared_ptr<std::vector<ProtocolServerAdapter>> servers{nullptr};

    mutable std::mutex mutex;
    std::condition_variable wait_cond;
    bool quit {false};
    AsynchronousMetricValues values;

#if defined(OS_LINUX)
    MemoryStatisticsOS memory_stat;
#endif

    std::unique_ptr<ThreadFromGlobalPool> thread;

    void run();
    void update();
};

}
