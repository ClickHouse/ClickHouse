#pragma once

#include <Backups/IRestoreCoordination.h>
#include <Backups/BackupConcurrencyCheck.h>
#include <Backups/BackupCoordinationCleaner.h>
#include <Backups/BackupCoordinationStageSync.h>
#include <Backups/WithRetries.h>


namespace DB
{

/// Implementation of the IRestoreCoordination interface performing coordination via ZooKeeper. It's necessary for "RESTORE ON CLUSTER".
class RestoreCoordinationOnCluster : public IRestoreCoordination
{
public:
    /// Empty string as the current host is used to mark the initiator of a RESTORE ON CLUSTER query.
    static const constexpr std::string_view kInitiator;

    RestoreCoordinationOnCluster(
        const UUID & restore_uuid_,
        const String & root_zookeeper_path_,
        zkutil::GetZooKeeper get_zookeeper_,
        const BackupKeeperSettings & keeper_settings_,
        const String & current_host_,
        const Strings & all_hosts_,
        bool allow_concurrent_restore_,
        BackupConcurrencyCounters & concurrency_counters_,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_,
        QueryStatusPtr process_list_element_);

    ~RestoreCoordinationOnCluster() override;

    Strings setStage(const String & new_stage, const String & message, bool sync) override;
    void setRestoreQueryWasSentToOtherHosts() override;
    bool trySetError(std::exception_ptr exception) override;
    void finish() override;
    bool tryFinishAfterError() noexcept override;
    void waitForOtherHostsToFinish() override;
    bool tryWaitForOtherHostsToFinishAfterError() noexcept override;

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

    /// Sets that this table is going to restore data into Keeper for all KeeperMap tables defined on root_zk_path.
    /// The function returns false if data for this specific root path is already being restored by another table.
    bool acquireInsertingDataForKeeperMap(const String & root_zk_path, const String & table_unique_id) override;

    /// Generates a new UUID for a table. The same UUID must be used for a replicated table on each replica,
    /// (because otherwise the macro "{uuid}" in the ZooKeeper path will not work correctly).
    void generateUUIDForTable(ASTCreateQuery & create_query) override;

    ZooKeeperRetriesInfo getOnClusterInitializationKeeperRetriesInfo() const override;

private:
    void createRootNodes();
    bool tryFinishImpl() noexcept;

    const String root_zookeeper_path;
    const BackupKeeperSettings keeper_settings;
    const UUID restore_uuid;
    const String zookeeper_path;
    const Strings all_hosts;
    const Strings all_hosts_without_initiator;
    const String current_host;
    const size_t current_host_index;
    LoggerPtr const log;

    const WithRetries with_retries;
    BackupConcurrencyCheck concurrency_check;
    BackupCoordinationStageSync stage_sync;
    BackupCoordinationCleaner cleaner;
    std::atomic<bool> restore_query_was_sent_to_other_hosts = false;
};

}
