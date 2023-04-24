#include <ctime>
#include <Backups/BackupRestoreCleanupThread.h>
#include "Backups/BackupsWorker.h"

namespace DB
{


BackupRestoreCleanupThread::BackupRestoreCleanupThread(
    zkutil::GetZooKeeper get_zookeeper_,
    BackgroundSchedulePool & pool_,
    String root_zookeeper_path_,
    UInt64 check_period_ms_,
    UInt64 timeout_to_cleanup_ms_,
    UInt64 consecutive_failed_checks_to_be_stale_)
    : get_zookeeper(get_zookeeper_)
    , log(&Poco::Logger::get("BackupRestoreCleanupThread"))
    , root_zookeeper_path(root_zookeeper_path_)
    , check_period_ms(check_period_ms_)
    , timeout_to_cleanup_ms(timeout_to_cleanup_ms_)
    , consecutive_failed_checks_to_be_stale(consecutive_failed_checks_to_be_stale_)
{
    task = pool_.createTask("BackupRestoreCleanupThread", [this]{ run(); });
    LOG_INFO(log, "Clenup thread initialized");
}

void BackupRestoreCleanupThread::run()
{
    if (need_stop)
        return;

    try
    {
        runImpl();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup stale backups and restores. Will try again alter");
    }

    task->scheduleAfter(check_period_ms);
}


void BackupRestoreCleanupThread::runImpl()
{
    LOG_INFO(log, "Cleaning up stale nodes from ZooKeeper");

    auto zk = get_zookeeper();
    zk->createAncestors(root_zookeeper_path);
    zk->createIfNotExists(root_zookeeper_path, "");

    for (const auto & operation_name : zk->getChildren(root_zookeeper_path))
    {
        bool is_alive = false;
        const auto path_to_stage_for_current_backup = root_zookeeper_path + "/" + operation_name + "/stage";

        if (zk->exists(path_to_stage_for_current_backup))
        {
            /// We also need to check whether there is at least one alive host
            /// Because backup could be interrupted in the middle and then ClickHouse
            /// will simply forget about it.
            for (const auto & child : zk->getChildren(path_to_stage_for_current_backup))
            {
                if (child.contains("alive"))
                {
                    is_alive = true;
                    break;
                }
            }
        }

        if (is_alive)
        {
            if (dead_counter.contains(operation_name))
                dead_counter.erase(operation_name);

            continue;
        }

        dead_counter[operation_name] += 1;
        if (dead_counter[operation_name] < consecutive_failed_checks_to_be_stale)
            continue;

        const auto full_path_for_current_operation = root_zookeeper_path + "/" + operation_name;

        /// Modify the mtime of the node
        Coordination::Stat root_stat;
        zk->set(full_path_for_current_operation, "");
        zk->get(full_path_for_current_operation, &root_stat);

        /// Let the Keeper decide what time is now
        const auto now = static_cast<UInt64>(root_stat.mtime);
        auto last_active_time = static_cast<UInt64>(root_stat.ctime);

        if (zk->exists(path_to_stage_for_current_backup))
        {
            Coordination::Stat stage_stat;
            zk->get(full_path_for_current_operation, &stage_stat);
            last_active_time = static_cast<UInt64>(stage_stat.mtime);
        }

        /// Check whether timeout is passed
        if (now >= timeout_to_cleanup_ms + last_active_time)
        {
            LOG_INFO(log, "Removing stale nodes under the path: {}", full_path_for_current_operation);
            /// We can safely remove this node.
            zk->removeRecursive(full_path_for_current_operation);
        }

        dead_counter.erase(operation_name);
    }
}


void BackupRestoreCleanupThread::shutdown()
{
    /// Stop restarting_thread before stopping other tasks - so that it won't restart them again.
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "Cleanup thread finished");
}


}
