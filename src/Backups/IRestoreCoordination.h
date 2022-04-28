#pragma once

#include <base/types.h>


namespace DB
{

/// Keeps information about files contained in a backup.
class IRestoreCoordination
{
public:
    virtual ~IRestoreCoordination() = default;

    /// Sets or gets path in the backup for a specified path in ZooKeeper.
    virtual void setOrGetPathInBackupForZkPath(const String & zk_path_, String & path_in_backup_) = 0;

    /// Sets that this replica is going to restore a partition in a replicated table or a table in a replicated database.
    /// This function should be called to prevent other replicas from doing that in parallel.
    virtual bool acquireZkPathAndName(const String & zk_path_, const String & name_) = 0;

    enum Result
    {
        SUCCEEDED,
        FAILED,
    };

    /// Sets the result for an acquired path and name.
    virtual void setResultForZkPathAndName(const String & zk_path_, const String & name_, Result res_) = 0;

    /// Waits for the result set by another replica for another replica's acquired path and name.
    /// Returns false if time is out.
    virtual bool getResultForZkPathAndName(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
