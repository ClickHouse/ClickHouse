#include <Backups/BackupCoordinationReplicatedWorkloadEntities.h>


namespace DB
{

BackupCoordinationReplicatedWorkloadEntities::BackupCoordinationReplicatedWorkloadEntities() = default;
BackupCoordinationReplicatedWorkloadEntities::~BackupCoordinationReplicatedWorkloadEntities() = default;

void BackupCoordinationReplicatedWorkloadEntities::addDirectory(DirectoryPathForWorkloadEntity && directory_path_for_workload_entity)
{
    const auto & loader_zk_path = directory_path_for_workload_entity.loader_zk_path;
    const auto & entity_type = directory_path_for_workload_entity.entity_type;
    const auto & host_id = directory_path_for_workload_entity.host_id;
    const auto & dir_path = directory_path_for_workload_entity.dir_path;

    auto & ref = dir_paths_by_zk_path[std::make_pair(loader_zk_path, entity_type)];
    ref.dir_paths.emplace(dir_path);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    ref.host_to_store = std::max(ref.host_to_store, host_id);
}

Strings BackupCoordinationReplicatedWorkloadEntities::getDirectories(const String & loader_zk_path, WorkloadEntityType entity_type, const String & host_id) const
{
    auto it = dir_paths_by_zk_path.find(std::make_pair(loader_zk_path, entity_type));
    if (it == dir_paths_by_zk_path.end())
        return {};

    const auto & dir_paths = it->second;
    if (dir_paths.host_to_store != host_id)
        return {};

    Strings res{dir_paths.dir_paths.begin(), dir_paths.dir_paths.end()};
    return res;
}

}
