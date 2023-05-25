#include <Backups/BackupCoordinationLocal.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <fmt/format.h>


namespace DB
{

BackupCoordinationLocal::BackupCoordinationLocal(bool plain_backup_)
    : log(&Poco::Logger::get("BackupCoordinationLocal")), file_infos(plain_backup_)
{
}

BackupCoordinationLocal::~BackupCoordinationLocal() = default;

void BackupCoordinationLocal::setStage(const String &, const String &)
{
}

void BackupCoordinationLocal::setError(const Exception &)
{
}

Strings BackupCoordinationLocal::waitForStage(const String &)
{
    return {};
}

Strings BackupCoordinationLocal::waitForStage(const String &, std::chrono::milliseconds)
{
    return {};
}

void BackupCoordinationLocal::addReplicatedPartNames(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name, const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    std::lock_guard lock{replicated_tables_mutex};
    replicated_tables.addPartNames({table_shared_id, table_name_for_logs, replica_name, part_names_and_checksums});
}

Strings BackupCoordinationLocal::getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{replicated_tables_mutex};
    return replicated_tables.getPartNames(table_shared_id, replica_name);
}


void BackupCoordinationLocal::addReplicatedMutations(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name, const std::vector<MutationInfo> & mutations)
{
    std::lock_guard lock{replicated_tables_mutex};
    replicated_tables.addMutations({table_shared_id, table_name_for_logs, replica_name, mutations});
}

std::vector<IBackupCoordination::MutationInfo> BackupCoordinationLocal::getReplicatedMutations(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{replicated_tables_mutex};
    return replicated_tables.getMutations(table_shared_id, replica_name);
}


void BackupCoordinationLocal::addReplicatedDataPath(const String & table_shared_id, const String & data_path)
{
    std::lock_guard lock{replicated_tables_mutex};
    replicated_tables.addDataPath({table_shared_id, data_path});
}

Strings BackupCoordinationLocal::getReplicatedDataPaths(const String & table_shared_id) const
{
    std::lock_guard lock{replicated_tables_mutex};
    return replicated_tables.getDataPaths(table_shared_id);
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


bool BackupCoordinationLocal::hasConcurrentBackups(const std::atomic<size_t> & num_active_backups) const
{
    if (num_active_backups > 1)
    {
        LOG_WARNING(log, "Found concurrent backups: num_active_backups={}", num_active_backups);
        return true;
    }
    return false;
}

}
