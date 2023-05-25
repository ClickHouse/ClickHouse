#pragma once

#include <Backups/IRestoreCoordination.h>
#include <mutex>
#include <set>
#include <unordered_set>

namespace Poco { class Logger; }


namespace DB
{

/// Implementation of the IRestoreCoordination interface performing coordination in memory.
class RestoreCoordinationLocal : public IRestoreCoordination
{
public:
    RestoreCoordinationLocal();
    ~RestoreCoordinationLocal() override;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    void setStage(const String & new_stage, const String & message) override;
    void setError(const Exception & exception) override;
    Strings waitForStage(const String & stage_to_wait) override;
    Strings waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout) override;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) override;

    /// Sets that this replica is going to restore a ReplicatedAccessStorage.
    /// The function returns false if this access storage is being already restored by another replica.
    bool acquireReplicatedAccessStorage(const String & access_storage_zk_path) override;

    /// Sets that this replica is going to restore replicated user-defined functions.
    /// The function returns false if user-defined function at a specified zk path are being already restored by another replica.
    bool acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type) override;

    bool hasConcurrentRestores(const std::atomic<size_t> & num_active_restores) const override;

private:
    Poco::Logger * const log;

    std::set<std::pair<String /* database_zk_path */, String /* table_name */>> acquired_tables_in_replicated_databases;
    std::unordered_set<String /* table_zk_path */> acquired_data_in_replicated_tables;
    mutable std::mutex mutex;
};

}
