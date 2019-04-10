#pragma once

#include <memory>
#include <ctime>
#include <cstddef>

#include <Core/BackgroundSchedulePool.h>


namespace DB
{

class Context;

/// Add a task to BackgroundProcessingPool that watch for ProfileEvents::NetworkErrors and updates DNS cache if it has increased
class DNSCacheUpdater
{
public:
    explicit DNSCacheUpdater(Context & context);

    /// Checks if it is a network error and increments ProfileEvents::NetworkErrors
    static bool incrementNetworkErrorEventsIfNeeded();

private:
    void run();

    Context & context;
    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder task_handle;

    size_t last_num_network_erros = 0;

    static constexpr size_t min_errors_to_update_cache = 3;
    static constexpr time_t min_update_period_seconds = 45;
};


}
