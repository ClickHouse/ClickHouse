#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class DiskLocal;

class DiskLocalCheckThread : WithContext
{
public:
    friend class DiskLocal;

    DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, UInt64 local_disk_check_period_ms);

    void startup();

    void shutdown();

private:
    bool check();
    void run();

    DiskLocal * disk;
    size_t check_period_ms;
    LoggerPtr log;
    std::atomic<bool> need_stop{false};

    BackgroundSchedulePool::TaskHolder task;
    size_t retry{};
};

}
