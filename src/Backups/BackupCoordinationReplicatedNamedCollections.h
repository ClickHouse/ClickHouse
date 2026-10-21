#pragma once

#include <Core/Types.h>
#include <map>
#include <unordered_set>


namespace DB
{

/// This class is used by hosts to coordinate the named collections they're going to write to a backup.
/// A replicated (Keeper) storage for named collections is shared by all the hosts using it, so this class
/// chooses one host to write the collections for all of them, the same way
/// BackupCoordinationReplicatedSQLObjects does for user-defined SQL objects.

class BackupCoordinationReplicatedNamedCollections
{
public:
    BackupCoordinationReplicatedNamedCollections();
    ~BackupCoordinationReplicatedNamedCollections();

    struct DirectoryPathForNamedCollections
    {
        String replication_id;
        String host_id;
        String dir_path;
    };

    /// Adds a path to a directory keeping named collections.
    void addDirectory(DirectoryPathForNamedCollections && directory_path);

    /// Returns all added paths to directories if `host_id` is the host chosen to store the named collections.
    Strings getDirectories(const String & replication_id, const String & host_id) const;

private:
    struct DirPathsAndHost
    {
        std::unordered_set<String> dir_paths;
        String host_to_store;
    };

    std::map<String, DirPathsAndHost> dir_paths_by_replication_id;
};

}
