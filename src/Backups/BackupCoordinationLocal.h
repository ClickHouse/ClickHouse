#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationFileInfos.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationReplicatedSQLObjects.h>
#include <Backups/BackupCoordinationReplicatedTables.h>
#include <Backups/BackupCoordinationKeeperMapTables.h>
#include <base/defines.h>
#include <cstddef>
#include <mutex>
#include <unordered_set>


namespace Poco { class Logger; }

namespace DB
{

/// Implementation of the IBackupCoordination interface performing coordination in memory.
class BackupCoordinationLocal : public IBackupCoordination
{
public:
    explicit BackupCoordinationLocal(bool plain_backup_);
    ~BackupCoordinationLocal() override;

    void setStage(const String & new_stage, const String & message) override;
    void setError(const Exception & exception) override;
    Strings waitForStage(const String & stage_to_wait) override;
    Strings waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout) override;

    void addReplicatedPartNames(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name,
                                const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;
    Strings getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedMutations(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name,
                                const std::vector<MutationInfo> & mutations) override;
    std::vector<MutationInfo> getReplicatedMutations(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_shared_id, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_shared_id) const override;

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

private:
    LoggerPtr const log;

    BackupCoordinationReplicatedTables TSA_GUARDED_BY(replicated_tables_mutex) replicated_tables;
    BackupCoordinationReplicatedAccess TSA_GUARDED_BY(replicated_access_mutex) replicated_access;
    BackupCoordinationReplicatedSQLObjects TSA_GUARDED_BY(replicated_sql_objects_mutex) replicated_sql_objects;
    BackupCoordinationFileInfos TSA_GUARDED_BY(file_infos_mutex) file_infos;
    BackupCoordinationKeeperMapTables keeper_map_tables TSA_GUARDED_BY(keeper_map_tables_mutex);
    std::unordered_set<size_t> TSA_GUARDED_BY(writing_files_mutex) writing_files;

    mutable std::mutex replicated_tables_mutex;
    mutable std::mutex replicated_access_mutex;
    mutable std::mutex replicated_sql_objects_mutex;
    mutable std::mutex file_infos_mutex;
    mutable std::mutex writing_files_mutex;
    mutable std::mutex keeper_map_tables_mutex;
};

}
