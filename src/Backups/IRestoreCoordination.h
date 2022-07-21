#pragma once

#include <Core/Types.h>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/// Replicas use this class to coordinate what they're reading from a backup while executing RESTORE ON CLUSTER.
/// There are two implementation of this interface: RestoreCoordinationLocal and RestoreCoordinationRemote.
/// RestoreCoordinationLocal is used while executing RESTORE without ON CLUSTER and performs coordination in memory.
/// RestoreCoordinationRemote is used while executing RESTORE with ON CLUSTER and performs coordination via ZooKeeper.
class IRestoreCoordination
{
public:
    virtual ~IRestoreCoordination() = default;

    /// Sets the current status and waits for other hosts to come to this status too. If status starts with "error:" it'll stop waiting on all the hosts.
    virtual void setStatus(const String & current_host, const String & new_status, const String & message) = 0;
    virtual Strings setStatusAndWait(const String & current_host, const String & new_status, const String & message, const Strings & other_hosts) = 0;
    virtual Strings setStatusAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & other_hosts, UInt64 timeout_ms) = 0;

    static constexpr const char * kErrorStatus = "error";

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    virtual bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) = 0;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    virtual bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) = 0;

    /// Sets that this replica is going to restore a ReplicatedAccessStorage.
    /// The function returns false if this access storage is being already restored by another replica.
    virtual bool acquireReplicatedAccessStorage(const String & access_storage_zk_path) = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
