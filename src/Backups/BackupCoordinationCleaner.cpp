#include <Backups/BackupCoordinationCleaner.h>


namespace DB
{

BackupCoordinationCleaner::BackupCoordinationCleaner(bool is_restore_, const String & zookeeper_path_, const WithRetries & with_retries_, LoggerPtr log_)
    : is_restore(is_restore_), zookeeper_path(zookeeper_path_), with_retries(with_retries_), log(log_)
{
}

void BackupCoordinationCleaner::cleanup(bool throw_if_error)
{
    WithRetries::Kind retries_kind = throw_if_error ? WithRetries::kNormal : WithRetries::kErrorHandling;
    cleanupImpl(throw_if_error, retries_kind);
}

void BackupCoordinationCleaner::cleanupImpl(bool throw_if_error, WithRetries::Kind retries_kind)
{
    {
        std::lock_guard lock{mutex};
        if (succeeded)
        {
            LOG_TRACE(log, "Nodes from ZooKeeper are already removed");
            return;
        }
        if (tried)
        {
            LOG_INFO(log, "Skipped removing nodes from ZooKeeper because because earlier we failed to do that");
            return;
        }
    }

    auto component_guard = Coordination::setCurrentComponent("BackupCoordinationCleaner::cleanupImpl");
    try
    {
        LOG_TRACE(log, "Removing nodes from ZooKeeper");
        auto holder = with_retries.createRetriesControlHolder("removeAllNodes", retries_kind);
        holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            zookeeper->removeRecursive(zookeeper_path);
        });

        std::lock_guard lock{mutex};
        tried = true;
        succeeded = true;
    }
    catch (...)
    {
        LOG_TRACE(log, "Caught exception while removing nodes from ZooKeeper for this {}: {}",
                  is_restore ? "restore" : "backup",
                  getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));

        std::lock_guard lock{mutex};
        tried = true;

        if (throw_if_error)
            throw;
    }
}

}
