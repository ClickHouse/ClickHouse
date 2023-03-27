#include <Backups/BackupCoordinationLocal.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_ENTRY_ALREADY_EXISTS;
}

using FileInfo = IBackupCoordination::FileInfo;

BackupCoordinationLocal::BackupCoordinationLocal(bool plain_backup_) : plain_backup(plain_backup_)
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
    std::lock_guard lock{mutex};
    replicated_tables.addPartNames(table_shared_id, table_name_for_logs, replica_name, part_names_and_checksums);
}

Strings BackupCoordinationLocal::getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{mutex};
    return replicated_tables.getPartNames(table_shared_id, replica_name);
}


void BackupCoordinationLocal::addReplicatedMutations(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name, const std::vector<MutationInfo> & mutations)
{
    std::lock_guard lock{mutex};
    replicated_tables.addMutations(table_shared_id, table_name_for_logs, replica_name, mutations);
}

std::vector<IBackupCoordination::MutationInfo> BackupCoordinationLocal::getReplicatedMutations(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{mutex};
    return replicated_tables.getMutations(table_shared_id, replica_name);
}


void BackupCoordinationLocal::addReplicatedDataPath(const String & table_shared_id, const String & data_path)
{
    std::lock_guard lock{mutex};
    replicated_tables.addDataPath(table_shared_id, data_path);
}

Strings BackupCoordinationLocal::getReplicatedDataPaths(const String & table_shared_id) const
{
    std::lock_guard lock{mutex};
    return replicated_tables.getDataPaths(table_shared_id);
}


void BackupCoordinationLocal::addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path)
{
    std::lock_guard lock{mutex};
    replicated_access.addFilePath(access_zk_path, access_entity_type, "", file_path);
}

Strings BackupCoordinationLocal::getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type) const
{
    std::lock_guard lock{mutex};
    return replicated_access.getFilePaths(access_zk_path, access_entity_type, "");
}


void BackupCoordinationLocal::addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path)
{
    std::lock_guard lock{mutex};
    replicated_sql_objects.addDirectory(loader_zk_path, object_type, "", dir_path);
}

Strings BackupCoordinationLocal::getReplicatedSQLObjectsDirs(const String & loader_zk_path, UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock{mutex};
    return replicated_sql_objects.getDirectories(loader_zk_path, object_type, "");
}


void BackupCoordinationLocal::addFileInfo(const FileInfo & file_info, bool & is_data_file_required)
{
    FileInfo inserting_info = file_info;

    std::lock_guard lock{mutex};

    if (plain_backup)
    {
        /// Plain backups don't use base backups and store each file as is.
        inserting_info.base_checksum = 0;
        inserting_info.base_size = 0;
        is_data_file_required = true;
    }
    else if (file_info.size == file_info.base_size)
    {
        /// The file is either empty or is taken from the base backup as a whole.
        inserting_info.data_file_name = "";
        is_data_file_required = false;
    }
    else if (auto it = file_infos.find(file_info); it != file_infos.end())
    {
        /// The file is a duplicate of another file in this backup.
        inserting_info.data_file_name = it->data_file_name;
        is_data_file_required = false;
    }
    else
    {
        /// The file is not empty, not a duplicate, and doesn't exist in the base backup as a whole,
        /// so it must be written to this backup.
        is_data_file_required = true;
    }

    file_infos.emplace(std::move(inserting_info));
}

std::vector<FileInfo> BackupCoordinationLocal::getAllFileInfos() const
{
    /// File infos are almost ready, we just need to sort them and check for duplicates.
    std::vector<FileInfo> res;
    {
        std::lock_guard lock{mutex};
        res.reserve(file_infos.size());
        std::copy(file_infos.begin(), file_infos.end(), std::back_inserter(res));
    }

    /// Sort file infos alphabetically and check there are no duplicates.
    std::sort(res.begin(), res.end(), FileInfo::LessByFileName{});

    if (auto adjacent_it = std::adjacent_find(res.begin(), res.end(), FileInfo::EqualByFileName{}); adjacent_it != res.end())
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Entry {} added multiple times to backup", quoteString(adjacent_it->file_name));
    }

    return res;
}


bool BackupCoordinationLocal::hasConcurrentBackups(const std::atomic<size_t> & num_active_backups) const
{
    return (num_active_backups > 1);
}

}
