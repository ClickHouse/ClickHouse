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

    using Value = double;
    using Container = std::unordered_map<std::string, Value>;

    /// Returns copy of all values.
    Container getValues() const;

private:
    Context & context;

    bool quit {false};
    std::mutex wait_mutex;
    std::condition_variable wait_cond;

    Container container;
    mutable std::mutex container_mutex;

#if defined(OS_LINUX)
    MemoryStatisticsOS memory_stat;
#endif

    ThreadFromGlobalPool thread;

    void run();
    void update();

    void set(const std::string & name, Value value);
};

}
