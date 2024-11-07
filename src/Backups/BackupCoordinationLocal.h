#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupConcurrencyCheck.h>
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
    explicit BackupCoordinationLocal(
        const UUID & backup_uuid_,
        bool is_plain_backup_,
        bool allow_concurrent_backup_,
        BackupConcurrencyCounters & concurrency_counters_);

    ~BackupCoordinationLocal() override;

    Strings setStage(const String &, const String &, bool) override { return {}; }
    void setBackupQueryWasSentToOtherHosts() override {}
    bool trySetError(std::exception_ptr) override { return true; }
    void finish() override {}
    bool tryFinishAfterError() noexcept override { return true; }
    void waitForOtherHostsToFinish() override {}
    bool tryWaitForOtherHostsToFinishAfterError() noexcept override { return true; }

    void addReplicatedPartNames(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name,
                                const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;
    Strings getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedMutations(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name,
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

    ZooKeeperRetriesInfo getOnClusterInitializationKeeperRetriesInfo() const override;

private:
    LoggerPtr const log;
    BackupConcurrencyCheck concurrency_check;

    BackupCoordinationReplicatedTables replicated_tables TSA_GUARDED_BY(replicated_tables_mutex);
    BackupCoordinationReplicatedAccess replicated_access TSA_GUARDED_BY(replicated_access_mutex);
    BackupCoordinationReplicatedSQLObjects replicated_sql_objects TSA_GUARDED_BY(replicated_sql_objects_mutex);
    BackupCoordinationFileInfos file_infos TSA_GUARDED_BY(file_infos_mutex);
    BackupCoordinationKeeperMapTables keeper_map_tables TSA_GUARDED_BY(keeper_map_tables_mutex);
    std::unordered_set<size_t> writing_files TSA_GUARDED_BY(writing_files_mutex);

    mutable std::mutex replicated_tables_mutex;
    mutable std::mutex replicated_access_mutex;
    mutable std::mutex replicated_sql_objects_mutex;
    mutable std::mutex file_infos_mutex;
    mutable std::mutex writing_files_mutex;
    mutable std::mutex keeper_map_tables_mutex;
};

}
