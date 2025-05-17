#include <Disks/DiskLocalCheckThread.h>

#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/formatReadable.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
extern const Metric ReadonlyDisks;
extern const Metric BrokenDisks;
}

namespace DB
{
static const auto DISK_CHECK_ERROR_SLEEP_MS = 1000;
static const auto DISK_CHECK_ERROR_RETRY_TIME = 3;

DiskLocalCheckThread::DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, UInt64 local_disk_check_period_ms)
    : WithContext(context_)
    , disk(std::move(disk_))
    , check_period_ms(local_disk_check_period_ms)
    , log(getLogger(fmt::format("DiskLocalCheckThread({})", disk->getName())))
{
    task = getContext()->getSchedulePool().createTask(log->name(), [this] { run(); });
}

void DiskLocalCheckThread::startup()
{
    need_stop = false;
    retry = 0;
    task->activateAndSchedule();
    LOG_INFO(
        log,
        "Disk check started with period {}",
        formatReadableTime(static_cast<double>(check_period_ms) * 1e6 /* ns */));
}

int diskStatusChange(bool old_val, bool new_val)
{
    return static_cast<int>(new_val) - static_cast<int>(old_val);
}

void DiskLocalCheckThread::run()
{
    if (need_stop)
        return;
    bool readonly = disk->isReadOnly();
    bool broken = disk->isBroken();
    bool can_read = disk->canRead();
    bool can_write = disk->canWrite();
    if (can_read)
    {
        if (disk->broken)
            LOG_INFO(log, "Disk seems to be fine. It can be recovered using `SYSTEM RESTART DISK {0}`", disk->getName());
        retry = 0;
        if (can_write)
            disk->readonly = false;
        else
        {
            disk->readonly = true;
            LOG_INFO(log, "Disk is readonly");
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
        if (!disk->broken)
            LOG_ERROR(log, "Disk marked as broken");
        else
            LOG_INFO(log, "Disk is still broken");
        disk->broken = true;
        task->scheduleAfter(check_period_ms);
    }
    CurrentMetrics::add(CurrentMetrics::ReadonlyDisks, diskStatusChange(readonly, disk->isReadOnly()));
    CurrentMetrics::add(CurrentMetrics::BrokenDisks, diskStatusChange(broken, disk->isBroken()));
}

void DiskLocalCheckThread::shutdown()
{
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "DiskLocalCheck thread finished");
}

}
