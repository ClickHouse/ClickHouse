#include <Disks/DiskLocalCheckThread.h>

#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>

namespace DB
{
static const auto DISK_CHECK_ERROR_SLEEP_MS = 1000;
static const auto DISK_CHECK_ERROR_RETRY_TIME = 3;

DiskLocalCheckThread::DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, UInt64 local_disk_check_period_ms)
    : WithContext(context_)
    , disk(std::move(disk_))
    , check_period_ms(local_disk_check_period_ms)
    , log_name(disk->getName() + "@" + disk->getPath() + " (DiskLocalCheckThread)")
    , log(&Poco::Logger::get(log_name))
{
}

void DiskLocalCheckThread::startup()
{
    task = getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
    need_stop = false;
    task->activateAndSchedule();
}

void DiskLocalCheckThread::run()
{
    if (need_stop)
        return;

    if (disk->setupAndCheck())
    {
        if (disk->broken)
            LOG_INFO(log, "Disk seems to be fine. It can be recovered using `SYSTEM RESTART DISK {}`", disk->getName());
        retry = 0;
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
        LOG_INFO(log, "Disk is broken");
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
