#pragma once

#include <memory>
#include <ctime>
#include <cstddef>


namespace DB
{

class Context;
class BackgroundProcessingPool;
class BackgroundProcessingPoolTaskInfo;


/// Add a task to BackgroundProcessingPool that watch for ProfileEvents::NetworkErrors and updates DNS cache if it has increased
class DNSCacheUpdater
{
public:

    explicit DNSCacheUpdater(Context & context);
    ~DNSCacheUpdater();

    /// Checks if it is a network error and increments ProfileEvents::NetworkErrors
    static bool incrementNetworkErrorEventsIfNeeded();

private:
    bool run();

    Context & context;
    BackgroundProcessingPool & pool;
    std::shared_ptr<BackgroundProcessingPoolTaskInfo> task_handle;
    size_t last_num_network_erros = 0;
    time_t last_update_time = 0;

    static constexpr size_t min_errors_to_update_cache = 3;
    static constexpr time_t min_update_period_seconds = 45;
};


}
