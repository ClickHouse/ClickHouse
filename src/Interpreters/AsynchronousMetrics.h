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


/** Periodically (each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  */
class AsynchronousMetrics
{
public:
    AsynchronousMetrics(Context & context_)
        : context(context_), thread([this] { run(); })
    {
    }

    ~AsynchronousMetrics();


    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

private:
    Context & context;

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
