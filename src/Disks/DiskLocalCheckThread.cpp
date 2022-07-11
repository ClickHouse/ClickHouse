#include <Disks/DiskLocalCheckThread.h>

#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace DB
{
static const auto DISK_CHECK_ERROR_SLEEP_MS = 1000;
static const auto DISK_CHECK_ERROR_RETRY_TIME = 3;

DiskLocalCheckThread::DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, UInt64 local_disk_check_period_ms)
    : WithContext(context_)
    , disk(std::move(disk_))
    , check_period_ms(local_disk_check_period_ms)
    , log(&Poco::Logger::get(fmt::format("DiskLocalCheckThread({})", disk->getName())))
{
    task = getContext()->getSchedulePool().createTask(log->name(), [this] { run(); });
}

void DiskLocalCheckThread::startup()
{
    need_stop = false;
    retry = 0;
    task->activateAndSchedule();
}

void DiskLocalCheckThread::run()
{
    if (need_stop)
        return;

    bool can_read = disk->canRead();
    bool can_write = disk->canWrite();
    if (can_read)
    {
        if (disk->broken)
            LOG_INFO(log, "Disk {0} seems to be fine. It can be recovered using `SYSTEM RESTART DISK {0}`", disk->getName());
        retry = 0;
        if (can_write)
            disk->readonly = false;
        else
        {
            disk->readonly = true;
            LOG_INFO(log, "Disk {} is readonly", disk->getName());
        }
        task->scheduleAfter(check_period_ms);
    }
    else if (!disk->broken && retry < DISK_CHECK_ERROR_RETRY_TIME)
    {
        ++retry;
        task->scheduleAfter(DISK_CHECK_ERROR_SLEEP_MS);
    }
    else
    {
        retry = 0;
        disk->broken = true;
        LOG_INFO(log, "Disk {} is broken", disk->getName());
        task->scheduleAfter(check_period_ms);
    }
}

void DiskLocalCheckThread::shutdown()
{
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "DiskLocalCheck thread finished");
}

}
