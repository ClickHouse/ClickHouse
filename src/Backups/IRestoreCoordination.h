#pragma once

#include <Core/Types.h>


namespace DB
{
enum class UserDefinedSQLObjectType : uint8_t;
class ASTCreateQuery;
struct ZooKeeperRetriesInfo;

/// Replicas use this class to coordinate what they're reading from a backup while executing RESTORE ON CLUSTER.
/// There are two implementation of this interface: RestoreCoordinationLocal and RestoreCoordinationOnCluster.
/// RestoreCoordinationLocal is used while executing RESTORE without ON CLUSTER and performs coordination in memory.
/// RestoreCoordinationOnCluster is used while executing RESTORE with ON CLUSTER and performs coordination via ZooKeeper.
class IRestoreCoordination
{
public:
    virtual ~IRestoreCoordination() = default;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    virtual Strings setStage(const String & new_stage, const String & message, bool sync) = 0;

    /// Sets that the restore query was sent to other hosts.
    /// Function waitForOtherHostsToFinish() will check that to find out if it should really wait or not.
    virtual void setRestoreQueryWasSentToOtherHosts() = 0;

    /// Lets other hosts know that the current host has encountered an error.
    virtual bool trySetError(std::exception_ptr exception) = 0;

    /// Lets other hosts know that the current host has finished its work.
    virtual void finish() = 0;

    /// Lets other hosts know that the current host has finished its work (as a part of error-handling process).
    virtual bool tryFinishAfterError() noexcept = 0;

    /// Waits until all the other hosts finish their work.
    /// Stops waiting and throws an exception if another host encounters an error or if some host gets cancelled.
    virtual void waitForOtherHostsToFinish() = 0;

    /// Waits until all the other hosts finish their work (as a part of error-handling process).
    /// Doesn't stops waiting if some host encounters an error or gets cancelled.
    virtual bool tryWaitForOtherHostsToFinishAfterError() noexcept = 0;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    virtual bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) = 0;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    virtual bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) = 0;

    /// Sets that this replica is going to restore a ReplicatedAccessStorage.
    /// The function returns false if this access storage is being already restored by another replica.
    virtual bool acquireReplicatedAccessStorage(const String & access_storage_zk_path) = 0;

    /// Sets that this replica is going to restore replicated user-defined functions.
    /// The function returns false if user-defined function at a specified zk path are being already restored by another replica.
    virtual bool acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type) = 0;

    /// Sets that this table is going to restore data into Keeper for all KeeperMap tables defined on root_zk_path.
    /// The function returns false if data for this specific root path is already being restored by another table.
    virtual bool acquireInsertingDataForKeeperMap(const String & root_zk_path, const String & table_unique_id) = 0;

    /// Generates a new UUID for a table. The same UUID must be used for a replicated table on each replica,
    /// (because otherwise the macro "{uuid}" in the ZooKeeper path will not work correctly).
    virtual void generateUUIDForTable(ASTCreateQuery & create_query) = 0;

    virtual ZooKeeperRetriesInfo getOnClusterInitializationKeeperRetriesInfo() const = 0;
};

}
