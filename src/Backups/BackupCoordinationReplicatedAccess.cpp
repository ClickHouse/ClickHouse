#include <Backups/BackupCoordinationReplicatedAccess.h>


namespace DB
{

BackupCoordinationReplicatedAccess::BackupCoordinationReplicatedAccess() = default;
BackupCoordinationReplicatedAccess::~BackupCoordinationReplicatedAccess() = default;

void BackupCoordinationReplicatedAccess::addFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path)
{
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
    if (file_paths.host_to_store_access != host_id)
        return {};

    Strings res{file_paths.file_paths.begin(), file_paths.file_paths.end()};
    return res;
}

}
