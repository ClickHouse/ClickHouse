#include <Backups/BackupCoordinationLocal.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>


namespace DB
{

using SizeAndChecksum = IBackupCoordination::SizeAndChecksum;
using FileInfo = IBackupCoordination::FileInfo;

BackupCoordinationLocal::BackupCoordinationLocal() = default;
BackupCoordinationLocal::~BackupCoordinationLocal() = default;

void BackupCoordinationLocal::setStatus(const String &, const String &, const String &)
{
}

void BackupCoordinationLocal::setErrorStatus(const String &, const Exception &)
{
}

Strings BackupCoordinationLocal::waitStatus(const Strings &, const String &)
{
    return {};
}

Strings BackupCoordinationLocal::waitStatusFor(const Strings &, const String &, UInt64)
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


void BackupCoordinationLocal::addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path)
{
    std::lock_guard lock{mutex};
    replicated_access.addFilePath(access_zk_path, access_entity_type, host_id, file_path);
}

Strings BackupCoordinationLocal::getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const
{
    std::lock_guard lock{mutex};
    return replicated_access.getFilePaths(access_zk_path, access_entity_type, host_id);
}


void BackupCoordinationLocal::addFileInfo(const FileInfo & file_info, bool & is_data_file_required)
{
    std::lock_guard lock{mutex};
    file_names.emplace(file_info.file_name, std::pair{file_info.size, file_info.checksum});
    if (!file_info.size)
    {
        is_data_file_required = false;
        return;
    }
    bool inserted_file_info = file_infos.try_emplace(std::pair{file_info.size, file_info.checksum}, file_info).second;
    is_data_file_required = inserted_file_info && (file_info.size > file_info.base_size);
}

void BackupCoordinationLocal::updateFileInfo(const FileInfo & file_info)
{
    if (!file_info.size)
        return; /// we don't keep FileInfos for empty files, nothing to update

    std::lock_guard lock{mutex};
    auto & dest = file_infos.at(std::pair{file_info.size, file_info.checksum});
    dest.archive_suffix = file_info.archive_suffix;
}

std::vector<FileInfo> BackupCoordinationLocal::getAllFileInfos() const
{
    std::lock_guard lock{mutex};
    std::vector<FileInfo> res;
    for (const auto & [file_name, size_and_checksum] : file_names)
    {
        FileInfo info;
        UInt64 size = size_and_checksum.first;
        if (size) /// we don't keep FileInfos for empty files
            info = file_infos.at(size_and_checksum);
        info.file_name = file_name;
        res.push_back(std::move(info));
    }
    return res;
}

Strings BackupCoordinationLocal::listFiles(const String & directory, bool recursive) const
{
    std::lock_guard lock{mutex};
    String prefix = directory;
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';
    String terminator = recursive ? "" : "/";

    Strings elements;
    for (auto it = file_names.lower_bound(prefix); it != file_names.end(); ++it)
    {
        const String & name = it->first;
        if (!name.starts_with(prefix))
            break;
        size_t start_pos = prefix.length();
        size_t end_pos = String::npos;
        if (!terminator.empty())
            end_pos = name.find(terminator, start_pos);
        std::string_view new_element = std::string_view{name}.substr(start_pos, end_pos - start_pos);
        if (!elements.empty() && (elements.back() == new_element))
            continue;
        elements.push_back(String{new_element});
    }

    return elements;
}

bool BackupCoordinationLocal::hasFiles(const String & directory) const
{
    std::lock_guard lock{mutex};
    String prefix = directory;
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';

    auto it = file_names.lower_bound(prefix);
    if (it == file_names.end())
        return false;

    const String & name = it->first;
    return name.starts_with(prefix);
}

std::optional<FileInfo> BackupCoordinationLocal::getFileInfo(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto it = file_names.find(file_name);
    if (it == file_names.end())
        return std::nullopt;
    const auto & size_and_checksum = it->second;
    UInt64 size = size_and_checksum.first;
    FileInfo info;
    if (size) /// we don't keep FileInfos for empty files
        info = file_infos.at(size_and_checksum);
    info.file_name = file_name;
    return info;
}

std::optional<FileInfo> BackupCoordinationLocal::getFileInfo(const SizeAndChecksum & size_and_checksum) const
{
    std::lock_guard lock{mutex};
    auto it = file_infos.find(size_and_checksum);
    if (it == file_infos.end())
        return std::nullopt;
    return it->second;
}

std::optional<SizeAndChecksum> BackupCoordinationLocal::getFileSizeAndChecksum(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto it = file_names.find(file_name);
    if (it == file_names.end())
        return std::nullopt;
    return it->second;
}

String BackupCoordinationLocal::getNextArchiveSuffix()
{
    std::lock_guard lock{mutex};
    String new_archive_suffix = fmt::format("{:03}", ++current_archive_suffix); /// Outputs 001, 002, 003, ...
    archive_suffixes.push_back(new_archive_suffix);
    return new_archive_suffix;
}

Strings BackupCoordinationLocal::getAllArchiveSuffixes() const
{
    std::lock_guard lock{mutex};
    return archive_suffixes;
}

}
