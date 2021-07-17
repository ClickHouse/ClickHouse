#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Stopwatch.h>


namespace DB
{

/// Add a task to BackgroundProcessingPool that resolves all hosts and updates cache with constant period.
class DNSCacheUpdater : WithContext
{
public:
    DNSCacheUpdater(ContextPtr context, Int32 update_period_seconds_);
    ~DNSCacheUpdater();
    void start();

private:
    void run();

    Int32 update_period_seconds;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder task_handle;
};


}
