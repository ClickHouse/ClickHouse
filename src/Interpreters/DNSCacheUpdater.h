#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Add a task to BackgroundProcessingPool that resolves all hosts and updates cache with constant period.
class DNSCacheUpdater : WithContext
{
public:
    DNSCacheUpdater(ContextPtr context, Int32 update_period_seconds_, UInt32 max_consecutive_failures);
    ~DNSCacheUpdater();
    void start();

private:
    void run();

    Int32 update_period_seconds;
    UInt32 max_consecutive_failures;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder task_handle;
};


}
