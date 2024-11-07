#pragma once

#include <Backups/WithRetries.h>


namespace DB
{

/// Removes all the nodes from ZooKeeper used to coordinate a BACKUP ON CLUSTER operation or
/// a RESTORE ON CLUSTER operation (successful or not).
/// This class is used by BackupCoordinationOnCluster and RestoreCoordinationOnCluster to cleanup.
class BackupCoordinationCleaner
{
public:
    BackupCoordinationCleaner(const String & zookeeper_path_, const WithRetries & with_retries_, LoggerPtr log_);

    void cleanup();
    bool tryCleanupAfterError() noexcept;

private:
    bool tryRemoveAllNodes(bool throw_if_error, WithRetries::Kind retries_kind);

    const String zookeeper_path;

    /// A reference to a field of the parent object which is either BackupCoordinationOnCluster or RestoreCoordinationOnCluster.
    const WithRetries & with_retries;

    const LoggerPtr log;

    struct CleanupResult
    {
        bool succeeded = false;
        std::exception_ptr exception;
    };
    CleanupResult cleanup_result TSA_GUARDED_BY(mutex);

    std::mutex mutex;
};

}
