#include <Backups/BackupCoordinationLocal.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <fmt/format.h>


namespace DB
{

BackupCoordinationLocal::BackupCoordinationLocal(
    const UUID & backup_uuid_,
    bool is_plain_backup_,
    bool allow_concurrent_backup_,
    BackupConcurrencyCounters & concurrency_counters_)
    : log(getLogger("BackupCoordinationLocal"))
    , concurrency_check(backup_uuid_, /* is_restore = */ false, /* on_cluster = */ false, allow_concurrent_backup_, concurrency_counters_)
    , file_infos(is_plain_backup_)
{
}

BackupCoordinationLocal::~BackupCoordinationLocal() = default;

ZooKeeperRetriesInfo BackupCoordinationLocal::getOnClusterInitializationKeeperRetriesInfo() const
{
    return {};
}

void BackupCoordinationLocal::addReplicatedPartNames(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name, const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    std::lock_guard lock{replicated_tables_mutex};
    replicated_tables.addPartNames({table_zk_path, table_name_for_logs, replica_name, part_names_and_checksums});
}

Strings BackupCoordinationLocal::getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const
{
    std::lock_guard lock{replicated_tables_mutex};
    return replicated_tables.getPartNames(table_zk_path, replica_name);
}


void BackupCoordinationLocal::addReplicatedMutations(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name, const std::vector<MutationInfo> & mutations)
{
    std::lock_guard lock{replicated_tables_mutex};
    replicated_tables.addMutations({table_zk_path, table_name_for_logs, replica_name, mutations});
}

std::vector<IBackupCoordination::MutationInfo> BackupCoordinationLocal::getReplicatedMutations(const String & table_zk_path, const String & replica_name) const
{
    std::lock_guard lock{replicated_tables_mutex};
    return replicated_tables.getMutations(table_zk_path, replica_name);
}


void BackupCoordinationLocal::addReplicatedDataPath(const String & table_zk_path, const String & data_path)
{
    std::lock_guard lock{replicated_tables_mutex};
    replicated_tables.addDataPath({table_zk_path, data_path});
}

Strings BackupCoordinationLocal::getReplicatedDataPaths(const String & table_zk_path) const
{
    std::lock_guard lock{replicated_tables_mutex};
    return replicated_tables.getDataPaths(table_zk_path);
}


void BackupCoordinationLocal::addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path)
{
    std::lock_guard lock{replicated_access_mutex};
    replicated_access.addFilePath({access_zk_path, access_entity_type, "", file_path});
}

Strings BackupCoordinationLocal::getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type) const
{
    std::lock_guard lock{replicated_access_mutex};
    return replicated_access.getFilePaths(access_zk_path, access_entity_type, "");
}


void BackupCoordinationLocal::addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path)
{
    std::lock_guard lock{replicated_sql_objects_mutex};
    replicated_sql_objects.addDirectory({loader_zk_path, object_type, "", dir_path});
}

Strings BackupCoordinationLocal::getReplicatedSQLObjectsDirs(const String & loader_zk_path, UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock{replicated_sql_objects_mutex};
    return replicated_sql_objects.getDirectories(loader_zk_path, object_type, "");
}

void BackupCoordinationLocal::addKeeperMapTable(const String & table_zookeeper_root_path, const String & table_id, const String & data_path_in_backup)
{
    std::lock_guard lock(keeper_map_tables_mutex);
    keeper_map_tables.addTable(table_zookeeper_root_path, table_id, data_path_in_backup);
}

String BackupCoordinationLocal::getKeeperMapDataPath(const String & table_zookeeper_root_path) const
{
    std::lock_guard lock(keeper_map_tables_mutex);
    return keeper_map_tables.getDataPath(table_zookeeper_root_path);
}


void BackupCoordinationLocal::addFileInfos(BackupFileInfos && file_infos_)
{
    std::lock_guard lock{file_infos_mutex};
    file_infos.addFileInfos(std::move(file_infos_), "");
}

BackupFileInfos BackupCoordinationLocal::getFileInfos() const
{
    std::lock_guard lock{file_infos_mutex};
    return file_infos.getFileInfos("");
}

BackupFileInfos BackupCoordinationLocal::getFileInfosForAllHosts() const
{
    std::lock_guard lock{file_infos_mutex};
    return file_infos.getFileInfosForAllHosts();
}

bool BackupCoordinationLocal::startWritingFile(size_t data_file_index)
{
    std::lock_guard lock{writing_files_mutex};
    /// Return false if this function was already called with this `data_file_index`.
    return writing_files.emplace(data_file_index).second;
}

}
