#include <Backups/BackupCoordinationReplicatedSQLObjects.h>


namespace DB
{

BackupCoordinationReplicatedSQLObjects::BackupCoordinationReplicatedSQLObjects() = default;
BackupCoordinationReplicatedSQLObjects::~BackupCoordinationReplicatedSQLObjects() = default;

void BackupCoordinationReplicatedSQLObjects::addDirectory(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & host_id, const String & dir_path)
{
    auto & ref = dir_paths_by_zk_path[std::make_pair(loader_zk_path, object_type)];
    ref.dir_paths.emplace(dir_path);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    ref.host_to_store = std::max(ref.host_to_store, host_id);
}

Strings BackupCoordinationReplicatedSQLObjects::getDirectories(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & host_id) const
{
    auto it = dir_paths_by_zk_path.find(std::make_pair(loader_zk_path, object_type));
    if (it == dir_paths_by_zk_path.end())
        return {};

    const auto & dir_paths = it->second;
    if (dir_paths.host_to_store != host_id)
        return {};

    Strings res{dir_paths.dir_paths.begin(), dir_paths.dir_paths.end()};
    return res;
}

}
