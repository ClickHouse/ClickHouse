#include <Disks/DiskLocalCheckThread.h>
#include <Disks/DiskLocal.h>

#include <Interpreters/Context.h>

#include <IO/WriteHelpers.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerUUID.h>

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

static const auto DISK_CHECK_ERROR_SLEEP_MS = 1000;

DiskLocalCheckThread::DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, UInt64 local_disk_check_period_ms)
    : WithContext(context_)
    , disk(std::move(disk_))
    , check_period_ms(local_disk_check_period_ms)
    , log(getLogger(fmt::format("{}::DiskLocalCheckThread", disk->getName())))
    , is_readonly(CurrentMetrics::ReadonlyDisks)
    , is_broken(CurrentMetrics::BrokenDisks)
{
    task = getContext()->getSchedulePool().createTask(StorageID::createEmpty(), log->name(), [this] { run(); });
    task->deactivate();
}

DiskLocalCheckThread::~DiskLocalCheckThread()
{
    task->deactivate();
}

void DiskLocalCheckThread::startup()
{
    task->activateAndSchedule();
    LOG_INFO(log, "Disk check for disk {} started with period {}", disk->getName(), formatReadableTime(static_cast<double>(check_period_ms) * 1e6));
}

void DiskLocalCheckThread::run()
{
    int64_t schedule_after = check_period_ms;

    try
    {
        const String path = fmt::format("clickhouse_disk_checker_{}", toString(DB::ServerUUID::get()));
        disk->checkAccessImpl(path);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        schedule_after = DISK_CHECK_ERROR_SLEEP_MS;
    }

    is_readonly.set(disk->isReadOnly());
    is_broken.set(disk->isBroken());
    task->scheduleAfter(schedule_after);
}

void DiskLocalCheckThread::shutdown()
{
    task->deactivate();
    LOG_TRACE(log, "DiskLocalCheck thread for disk {} finished", disk->getName());
}

}
