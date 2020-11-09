
#include <Core/BackgroundSchedulePool.h>
#include <Common/Stopwatch.h>


namespace DB
{

class Context;

/// Add a task to BackgroundProcessingPool that resolves all hosts and updates cache with constant period.
class DNSCacheUpdater
{
public:
    explicit DNSCacheUpdater(Context & context, Int32 update_period_seconds_);
    ~DNSCacheUpdater();
    void start();

private:
    void run();

    Context & context;
    Int32 update_period_seconds;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder task_handle;
};


}
