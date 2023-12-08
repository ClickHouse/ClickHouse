#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationFileInfos.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationReplicatedSQLObjects.h>
#include <Backups/BackupCoordinationReplicatedTables.h>
#include <Backups/BackupCoordinationKeeperMapTables.h>
#include <Backups/BackupCoordinationStageSync.h>
#include <Backups/WithRetries.h>


namespace DB
{

/// We try to store data to zookeeper several times due to possible version conflicts.
constexpr size_t MAX_ZOOKEEPER_ATTEMPTS = 10;

/// Implementation of the IBackupCoordination interface performing coordination via ZooKeeper. It's necessary for "BACKUP ON CLUSTER".
class BackupCoordinationRemote : public IBackupCoordination
{
public:
    using BackupKeeperSettings = WithRetries::KeeperSettings;

    BackupCoordinationRemote(
        zkutil::GetZooKeeper get_zookeeper_,
        const String & root_zookeeper_path_,
        const BackupKeeperSettings & keeper_settings_,
        const String & backup_uuid_,
        const Strings & all_hosts_,
        const String & current_host_,
        bool plain_backup_,
        bool is_internal_,
        QueryStatusPtr process_list_element_);

    ~BackupCoordinationRemote() override;

    void setStage(const String & new_stage, const String & message) override;
    void setError(const Exception & exception) override;
    Strings waitForStage(const String & stage_to_wait) override;
    Strings waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout) override;

    void addReplicatedPartNames(
        const String & table_zk_path,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;

    Strings getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedMutations(
        const String & table_zk_path,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<MutationInfo> & mutations) override;

    std::vector<MutationInfo> getReplicatedMutations(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_zk_path, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_zk_path) const override;

    void addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path) override;
    Strings getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type) const override;

    void addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path) override;
    Strings getReplicatedSQLObjectsDirs(const String & loader_zk_path, UserDefinedSQLObjectType object_type) const override;

    void addKeeperMapTable(const String & table_zookeeper_root_path, const String & table_id, const String & data_path_in_backup) override;
    String getKeeperMapDataPath(const String & table_zookeeper_root_path) const override;

    void addFileInfos(BackupFileInfos && file_infos) override;
    BackupFileInfos getFileInfos() const override;
    BackupFileInfos getFileInfosForAllHosts() const override;
    bool startWritingFile(size_t data_file_index) override;

    bool hasConcurrentBackups(const std::atomic<size_t> & num_active_backups) const override;

    static size_t findCurrentHostIndex(const Strings & all_hosts, const String & current_host);

private:
    void createRootNodes();
    void removeAllNodes();

    void serializeToMultipleZooKeeperNodes(const String & path, const String & value, const String & logging_name);
    String deserializeFromMultipleZooKeeperNodes(const String & path, const String & logging_name) const;

    /// Reads data of all objects from ZooKeeper that replicas have added to backup and add it to the corresponding
    /// BackupCoordinationReplicated* objects.
    /// After that, calling addReplicated* functions is not allowed and throws an exception.
    void prepareReplicatedTables() const TSA_REQUIRES(replicated_tables_mutex);
    void prepareReplicatedAccess() const TSA_REQUIRES(replicated_access_mutex);
    void prepareReplicatedSQLObjects() const TSA_REQUIRES(replicated_sql_objects_mutex);
    void prepareKeeperMapTables() const TSA_REQUIRES(keeper_map_tables_mutex);
    void prepareFileInfos() const TSA_REQUIRES(file_infos_mutex);

    const String root_zookeeper_path;
    const String zookeeper_path;
    const BackupKeeperSettings keeper_settings;
    const String backup_uuid;
    const Strings all_hosts;
    const String current_host;
    const size_t current_host_index;
    const bool plain_backup;
    const bool is_internal;
    LoggerPtr const log;

    /// The order of these two fields matters, because stage_sync holds a reference to with_retries object
    mutable WithRetries with_retries;
    std::optional<BackupCoordinationStageSync> stage_sync;

    mutable std::optional<BackupCoordinationReplicatedTables> TSA_GUARDED_BY(replicated_tables_mutex) replicated_tables;
    mutable std::optional<BackupCoordinationReplicatedAccess> TSA_GUARDED_BY(replicated_access_mutex) replicated_access;
    mutable std::optional<BackupCoordinationReplicatedSQLObjects> TSA_GUARDED_BY(replicated_sql_objects_mutex) replicated_sql_objects;
    mutable std::optional<BackupCoordinationFileInfos> TSA_GUARDED_BY(file_infos_mutex) file_infos;
    mutable std::optional<BackupCoordinationKeeperMapTables> keeper_map_tables TSA_GUARDED_BY(keeper_map_tables_mutex);
    std::unordered_set<size_t> TSA_GUARDED_BY(writing_files_mutex) writing_files;

    mutable std::mutex zookeeper_mutex;
    mutable std::mutex replicated_tables_mutex;
    mutable std::mutex replicated_access_mutex;
    mutable std::mutex replicated_sql_objects_mutex;
    mutable std::mutex file_infos_mutex;
    mutable std::mutex writing_files_mutex;
    mutable std::mutex keeper_map_tables_mutex;
};

}
