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

typedef double AsynchronousMetricValue;
typedef std::unordered_map<std::string, AsynchronousMetricValue> AsynchronousMetricValues;


/** Periodically (by default, each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  */
class AsynchronousMetrics
{
public:
    // The default value of update_period_seconds is for ClickHouse-over-YT
    // in Arcadia -- it uses its own server implementation that also uses these
    // metrics.
    AsynchronousMetrics(Context & context_, int update_period_seconds = 60)
        : context(context_),
          update_period(update_period_seconds),
          thread([this] { run(); })
    {
    }

    ~AsynchronousMetrics();


    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

private:
    Context & context;
    const std::chrono::seconds update_period;

    mutable std::mutex mutex;
    std::condition_variable wait_cond;
    bool quit {false};
    AsynchronousMetricValues values;

#if defined(OS_LINUX)
    MemoryStatisticsOS memory_stat;
#endif

    ThreadFromGlobalPool thread;

    void run();
    void update();
};

}
