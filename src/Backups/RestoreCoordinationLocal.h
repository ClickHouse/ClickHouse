#pragma once

#include <Backups/IRestoreCoordination.h>
#include <mutex>
#include <set>
#include <unordered_set>

namespace Poco { class Logger; }


namespace DB
{

class RestoreCoordinationLocal : public IRestoreCoordination
{
public:
    RestoreCoordinationLocal();
    ~RestoreCoordinationLocal() override;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    void syncStage(const String & current_host, int stage, const Strings & wait_hosts, std::chrono::seconds timeout) override;

    /// Sets that the current host encountered an error, so other hosts should know that and stop waiting in syncStage().
    void syncStageError(const String & current_host, const String & error_message) override;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) override;

private:
    std::set<std::pair<String /* database_zk_path */, String /* table_name */>> acquired_tables_in_replicated_databases;
    std::unordered_set<String /* table_zk_path */> acquired_data_in_replicated_tables;
    mutable std::mutex mutex;
};

}
