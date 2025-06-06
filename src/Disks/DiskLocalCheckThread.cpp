#include <Disks/DiskLocalCheckThread.h>

#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/formatReadable.h>
#include <Poco/DirectoryIterator.h>

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
        "Disk check started for disk [{}] with period {}",
        disk->getName(),
        formatReadableTime(static_cast<double>(check_period_ms) * 1e6 /* ns */));
}

void DiskLocalCheckThread::run()
{
    if (need_stop)
        return;

    bool can_read = disk->canRead();
    bool can_write = disk->canWrite();
    LOG_INFO(log, "Disk {} seems to be readable: {} . writable: {}", disk->getName(), can_read, can_write);
    if (can_read)
    {
        if (disk->getName() == "test1" || disk->getName() == "test2")
        {
            std::string disk_path =
                (disk->getName() == "test1" ? "/var/lib/clickhouse/path1/" : "/var/lib/clickhouse/path2/");
            try
            {
                Poco::DirectoryIterator end;
                bool found = false;
                for (Poco::DirectoryIterator it(disk_path); it != end; ++it)
                {
                    LOG_INFO(log, "DiskLocalCheckThread found file for disk {} in path {}: {}",
                             disk->getName(), disk_path, it.name());
                    found = true;
                }
                if(!found) {
                    LOG_INFO(log, "DiskLocalCheckThread did not found file for disk {} in path {}",
                             disk->getName(), disk_path);
                }
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(log, "Failed to list {}: {}", disk_path, e.what());
            }
        }

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
        LOG_INFO(log, "Disk {} becomes broken. not reached DISK_CHECK_ERROR_RETRY_TIME", disk->getName());
        ++retry;
        task->scheduleAfter(DISK_CHECK_ERROR_SLEEP_MS);
    }
    else
    {
        retry = 0;
        if (!disk->broken)
            LOG_ERROR(log, "Disk {} marked as broken", disk->getName());
        else
            LOG_INFO(log, "Disk {} is still broken", disk->getName());
        disk->broken = true;
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
