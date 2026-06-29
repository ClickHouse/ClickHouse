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
    BackupCoordinationCleaner(bool is_restore_, const String & zookeeper_path_, const WithRetries & with_retries_, LoggerPtr log_);

    void cleanup(bool throw_if_error);

private:
    void cleanupImpl(bool throw_if_error, WithRetries::Kind retries_kind);

    const bool is_restore;
    const String zookeeper_path;

    /// A reference to a field of the parent object which is either BackupCoordinationOnCluster or RestoreCoordinationOnCluster.
    const WithRetries & with_retries;

    const LoggerPtr log;

    bool tried TSA_GUARDED_BY(mutex) = false;
    bool succeeded TSA_GUARDED_BY(mutex) = false;
    std::mutex mutex;
};

}
