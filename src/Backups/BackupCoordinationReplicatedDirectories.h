#pragma once

#include <Core/Types.h>
#include <algorithm>
#include <map>
#include <unordered_set>


namespace DB
{

/// This class is used by hosts to coordinate the directories of a replicated (Keeper) storage they're going to write to a backup:
/// user-defined SQL objects, workload entities (WORKLOAD and RESOURCE) or named collections. `Key` identifies the storage and
/// the kind of objects in it. It's designed to make all hosts save the same objects to the backup even in case some objects
/// change while the backup is being produced. This is important to make RESTORE more predictable.
///
/// For example, let's consider three replicas having access to the user-defined function `f1`.
/// This class ensures that the following files in the backup will be the same:
/// /shards/1/replicas/1/data/system/functions/f1.sql
/// /shards/1/replicas/2/data/system/functions/f1.sql
/// /shards/1/replicas/3/data/system/functions/f1.sql
///
/// To implement that this class chooses one host to write the objects for all the hosts so in fact all those files
/// in the example above are written by single host.
template <typename Key>
class BackupCoordinationReplicatedDirectories
{
public:
    struct DirectoryPath
    {
        Key key;
        String host_id;
        String dir_path;
    };

    /// Adds a path to a directory keeping the objects identified by `key`.
    void addDirectory(DirectoryPath && directory_path)
    {
        auto & ref = dir_paths_by_key[directory_path.key];
        ref.dir_paths.emplace(directory_path.dir_path);

        /// `std::max` because the calculation must give the same result being repeated on a different replica.
        ref.host_to_store = std::max(ref.host_to_store, directory_path.host_id);
    }

    /// Returns all added paths to directories if `host_id` is the host chosen to store the objects identified by `key`.
    [[nodiscard]] Strings getDirectories(const Key & key, const String & host_id) const
    {
        auto it = dir_paths_by_key.find(key);
        if (it == dir_paths_by_key.end())
            return {};

        const auto & dir_paths = it->second;
        if (dir_paths.host_to_store != host_id)
            return {};

        return Strings{dir_paths.dir_paths.begin(), dir_paths.dir_paths.end()};
    }

private:
    struct DirPathsAndHost
    {
        std::unordered_set<String> dir_paths;
        String host_to_store;
    };

    std::map<Key, DirPathsAndHost> dir_paths_by_key;
};

}
