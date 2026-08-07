#include <Backups/BackupCoordinationReplicatedAccess.h>

#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

BackupCoordinationReplicatedAccess::BackupCoordinationReplicatedAccess() = default;
BackupCoordinationReplicatedAccess::~BackupCoordinationReplicatedAccess() = default;

void BackupCoordinationReplicatedAccess::addFilePath(FilePathForAccessEntity && file_path_for_access_entity)
{
    const auto & access_zk_path = file_path_for_access_entity.access_zk_path;
    const auto & access_entity_type = file_path_for_access_entity.access_entity_type;
    const auto & host_id = file_path_for_access_entity.host_id;
    const auto & file_path = file_path_for_access_entity.file_path;

    auto & ref = file_paths_by_zk_path[std::make_pair(access_zk_path, access_entity_type)];
    ref.file_paths.emplace(file_path);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    ref.host_to_store_access = std::max(ref.host_to_store_access, host_id);
}

Strings BackupCoordinationReplicatedAccess::getFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const
{
    auto it = file_paths_by_zk_path.find(std::make_pair(access_zk_path, access_entity_type));
    if (it == file_paths_by_zk_path.end())
        return {};

    const auto & file_paths = it->second;
    if ((file_paths.host_to_store_access != host_id) || file_paths.file_paths.empty())
        return {};

    /// Use the same filename for all the paths in backup.
    /// Those filenames have format "access-<UUID>.txt", where UUID is random.
    /// It's not really necessary, however it looks better if those files have the same filename
    /// for a backup of ReplicatedAccessStorage on different hosts.
    Strings res;
    res.reserve(file_paths.file_paths.size());
    String filename = fs::path{*file_paths.file_paths.begin()}.filename();
    for (const auto & file_path : file_paths.file_paths)
        res.emplace_back(fs::path{file_path}.replace_filename(filename));

    return res;
}

}
