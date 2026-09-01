#include <Disks/DiskLocalCheckThread.h>
#include <Disks/DiskLocal.h>

#include <Interpreters/Context.h>

#include <IO/WriteHelpers.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/UUID.h>

#include <Common/Logger.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>

namespace CurrentMetrics
{
    extern const Metric ReadonlyDisks;
    extern const Metric BrokenDisks;
}

namespace DB
{

DiskLocalCheckThread::DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, int64_t local_disk_check_period_ms)
    : WithContext(context_)
    , disk(std::move(disk_))
    , check_file_path(fmt::format("clickhouse_disk_checker_{}", toString(UUIDHelpers::generateV4())))
    , log(getLogger(fmt::format("{}::DiskLocalCheckThread", disk->getName())))
    , is_readonly(CurrentMetrics::ReadonlyDisks)
    , is_broken(CurrentMetrics::BrokenDisks)
{
    check_period.setConfiguration(static_cast<double>(local_disk_check_period_ms), static_cast<double>(local_disk_check_period_ms) * 10, 1.1);
    task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), log->name(), [this] { run(); });
    task->deactivate();
}

DiskLocalCheckThread::~DiskLocalCheckThread()
{
    task->deactivate();
}

void DiskLocalCheckThread::startup()
{
    LOG_INFO(log, "Disk check for disk {} started with period {}, check file {}", disk->getName(), formatReadableTime(static_cast<double>(check_period.getCurrentDelay()) * 1e6), check_file_path);
    task->activateAndSchedule();
}

void DiskLocalCheckThread::run()
{
    try
    {
        disk->checkAccessImpl(check_file_path);
        check_period.rotateToMin();
    }
    catch (...)
    {
        tryLogCurrentException(log);
        check_period.up();
    }

    is_readonly.set(disk->isReadOnly());
    is_broken.set(disk->isBroken());
    task->scheduleAfter(check_period.getCurrentDelay());
}

void DiskLocalCheckThread::shutdown()
{
    task->deactivate();
}

}
