#pragma once

#include <Core/Types.h>


namespace DB
{
class Exception;
enum class UserDefinedSQLObjectType : uint8_t;
class ASTCreateQuery;

/// Replicas use this class to coordinate what they're reading from a backup while executing RESTORE ON CLUSTER.
/// There are two implementation of this interface: RestoreCoordinationLocal and RestoreCoordinationRemote.
/// RestoreCoordinationLocal is used while executing RESTORE without ON CLUSTER and performs coordination in memory.
/// RestoreCoordinationRemote is used while executing RESTORE with ON CLUSTER and performs coordination via ZooKeeper.
class IRestoreCoordination
{
public:
    virtual ~IRestoreCoordination() = default;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    virtual void setStage(const String & new_stage, const String & message) = 0;
    virtual void setError(const Exception & exception) = 0;
    virtual Strings waitForStage(const String & stage_to_wait) = 0;
    virtual Strings waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout) = 0;

    static constexpr const char * kErrorStatus = "error";

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

    /// This function is used to check if concurrent restores are running
    /// other than the restore passed to the function
    virtual bool hasConcurrentRestores(const std::atomic<size_t> & num_active_restores) const = 0;
};

}
