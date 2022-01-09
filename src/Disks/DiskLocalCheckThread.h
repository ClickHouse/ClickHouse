#pragma once

#include <atomic>
#include <thread>

#include <Core/BackgroundSchedulePool.h>
#include <base/logger_useful.h>

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
    String log_name;
    Poco::Logger * log;
    std::atomic<bool> need_stop{false};

    BackgroundSchedulePool::TaskHolder task;
    size_t retry{};
    static constexpr size_t BUF_SIZE = 4096;
    alignas(BUF_SIZE) char wbuf[BUF_SIZE]{};
    alignas(BUF_SIZE) char rbuf[BUF_SIZE]{};
};

}
