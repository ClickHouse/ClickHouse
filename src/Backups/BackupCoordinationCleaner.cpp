#include <Backups/BackupCoordinationCleaner.h>


namespace DB
{

BackupCoordinationCleaner::BackupCoordinationCleaner(const String & zookeeper_path_, const WithRetries & with_retries_, LoggerPtr log_)
    : zookeeper_path(zookeeper_path_), with_retries(with_retries_), log(log_)
{
}

void BackupCoordinationCleaner::cleanup()
{
    tryRemoveAllNodes(/* throw_if_error = */ true, /* retries_kind = */ WithRetries::kNormal);
}

bool BackupCoordinationCleaner::tryCleanupAfterError() noexcept
{
    return tryRemoveAllNodes(/* throw_if_error = */ false, /* retries_kind = */ WithRetries::kNormal);
}

bool BackupCoordinationCleaner::tryRemoveAllNodes(bool throw_if_error, WithRetries::Kind retries_kind)
{
    {
        std::lock_guard lock{mutex};
        if (cleanup_result.succeeded)
            return true;
        if (cleanup_result.exception)
        {
            if (throw_if_error)
                std::rethrow_exception(cleanup_result.exception);
            return false;
        }
    }

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
        cleanup_result.succeeded = true;
        return true;
    }
    catch (...)
    {
        LOG_TRACE(log, "Caught exception while removing nodes from ZooKeeper for this restore: {}",
                  getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));

        std::lock_guard lock{mutex};
        cleanup_result.exception = std::current_exception();

        if (throw_if_error)
            throw;
        return false;
    }
}

}
