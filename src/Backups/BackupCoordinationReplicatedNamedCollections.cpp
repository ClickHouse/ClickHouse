#include <Backups/BackupCoordinationReplicatedNamedCollections.h>


namespace DB
{

BackupCoordinationReplicatedNamedCollections::BackupCoordinationReplicatedNamedCollections() = default;
BackupCoordinationReplicatedNamedCollections::~BackupCoordinationReplicatedNamedCollections() = default;

void BackupCoordinationReplicatedNamedCollections::addDirectory(DirectoryPathForNamedCollections && directory_path)
{
    auto & ref = dir_paths_by_replication_id[directory_path.replication_id];
    ref.dir_paths.emplace(directory_path.dir_path);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    ref.host_to_store = std::max(ref.host_to_store, directory_path.host_id);
}

Strings BackupCoordinationReplicatedNamedCollections::getDirectories(const String & replication_id, const String & host_id) const
{
    auto it = dir_paths_by_replication_id.find(replication_id);
    if (it == dir_paths_by_replication_id.end())
        return {};

    const auto & dir_paths = it->second;
    if (dir_paths.host_to_store != host_id)
        return {};

    return Strings{dir_paths.dir_paths.begin(), dir_paths.dir_paths.end()};
}

}
