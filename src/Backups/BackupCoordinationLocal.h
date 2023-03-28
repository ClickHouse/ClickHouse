#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationFileInfos.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationReplicatedSQLObjects.h>
#include <Backups/BackupCoordinationReplicatedTables.h>
#include <base/defines.h>
#include <mutex>
#include <set>


namespace Poco { class Logger; }

namespace DB
{

/// Implementation of the IBackupCoordination interface performing coordination in memory.
class BackupCoordinationLocal : public IBackupCoordination
{
public:
    BackupCoordinationLocal(bool plain_backup_);
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

    void addFileInfos(BackupFileInfos && file_infos) override;
    BackupFileInfos getFileInfos() const override;
    BackupFileInfos getFileInfosForAllHosts() const override;
    bool startWritingFile(size_t data_file_index) override;

    bool hasConcurrentBackups(const std::atomic<size_t> & num_active_backups) const override;

private:
    mutable std::mutex mutex;
    BackupCoordinationReplicatedTables replicated_tables TSA_GUARDED_BY(mutex);
    BackupCoordinationReplicatedAccess replicated_access TSA_GUARDED_BY(mutex);
    BackupCoordinationReplicatedSQLObjects replicated_sql_objects TSA_GUARDED_BY(mutex);
    BackupCoordinationFileInfos file_infos TSA_GUARDED_BY(mutex);
    std::unordered_set<size_t> writing_files TSA_GUARDED_BY(mutex);
};

}
