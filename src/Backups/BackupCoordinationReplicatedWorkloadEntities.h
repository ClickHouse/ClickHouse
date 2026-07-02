#pragma once

#include <Core/Types.h>
#include <map>
#include <unordered_set>


namespace DB
{
enum class WorkloadEntityType : uint8_t;

/// This class is used by hosts to coordinate the workload entities (WORKLOAD and RESOURCE) they're going to write to a backup.
/// It's designed to make all hosts save the same entities to the backup even in case some entities change while
/// the backup is being produced. This is important to make RESTORE more predictible.
///
/// For example, let's consider three replicas having access to the workload `all`.
/// This class ensures that the following files in the backup will be the same:
/// /shards/1/replicas/1/data/system/workloads/all.sql
/// /shards/1/replicas/2/data/system/workloads/all.sql
/// /shards/1/replicas/3/data/system/workloads/all.sql
///
/// To implement that this class chooses one host to write workload entities for all the hosts so in fact all those files
/// in the example above are written by single host.

class BackupCoordinationReplicatedWorkloadEntities
{
public:
    BackupCoordinationReplicatedWorkloadEntities();
    ~BackupCoordinationReplicatedWorkloadEntities();

    struct DirectoryPathForWorkloadEntity
    {
        String loader_zk_path;
        WorkloadEntityType entity_type;
        String host_id;
        String dir_path;
    };

    /// Adds a path to directory keeping workload entities.
    void addDirectory(DirectoryPathForWorkloadEntity && directory_path_for_workload_entity);

    /// Returns all added paths to directories if `host_id` is a host chosen to store workload entities.
    Strings getDirectories(const String & loader_zk_path, WorkloadEntityType entity_type, const String & host_id) const;

private:
    using ZkPathAndEntityType = std::pair<String, WorkloadEntityType>;

    struct DirPathsAndHost
    {
        std::unordered_set<String> dir_paths;
        String host_to_store;
    };

    std::map<ZkPathAndEntityType, DirPathsAndHost> dir_paths_by_zk_path;
};

}
