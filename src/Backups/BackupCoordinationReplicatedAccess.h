#pragma once

#include <Core/Types.h>
#include <map>
#include <set>


namespace DB
{
enum class AccessEntityType : uint8_t;

/// This class is used by hosts to coordinate the access entities of ReplicatedAccessStorage they're writing to a backup.
/// It's designed to make all hosts save the same access entities to the backup even in case the ReplicatedAccessStorage changes
/// while the backup is being produced. This is important to make RESTORE more predicitible.
///
/// For example, let's consider three replicas having a ReplicatedAccessStorage on them.
/// This class ensures that the following files in the backup are the same:
/// /shards/1/replicas/1/data/system/users/access01.txt
/// /shards/1/replicas/2/data/system/users/access01.txt
/// /shards/1/replicas/3/data/system/users/access01.txt
///
/// To implement that this class chooses one host to write access entities for all the hosts so in fact all those files
/// in the example above are written by the same host.

class BackupCoordinationReplicatedAccess
{
public:
    BackupCoordinationReplicatedAccess();
    ~BackupCoordinationReplicatedAccess();

    struct FilePathForAccessEntity
    {
        String access_zk_path;
        AccessEntityType access_entity_type;
        String host_id;
        String file_path;
    };

    /// Adds a path to access*.txt file keeping access entities of a ReplicatedAccessStorage.
    void addFilePath(FilePathForAccessEntity && file_path_for_access_entity);

    /// Returns all paths added by addFilePath() if `host_id` is a host chosen to store access.
    Strings getFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const;

private:
    using ZkPathAndEntityType = std::pair<String, AccessEntityType>;

    struct FilePathsAndHost
    {
        std::set<String> file_paths;
        String host_to_store_access;
    };

    std::map<ZkPathAndEntityType, FilePathsAndHost> file_paths_by_zk_path;
};

}
