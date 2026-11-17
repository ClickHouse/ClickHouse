#pragma once

#include <Core/Types.h>
#include <map>
#include <unordered_set>


namespace DB
{
enum class UserDefinedSQLObjectType : uint8_t;

/// This class is used by hosts to coordinate the user-defined SQL objects they're going to write to a backup.
/// It's designed to make all hosts save the same objects to the backup even in case some objects change while
/// the backup is being produced. This is important to make RESTORE more predicitible.
///
/// For example, let's consider three replicas having access to the user-defined function `f1`.
/// This class ensures that the following files in the backup will be the same:
/// /shards/1/replicas/1/data/system/functions/f1.sql
/// /shards/1/replicas/2/data/system/functions/f1.sql
/// /shards/1/replicas/3/data/system/functions/f1.sql
///
/// To implement that this class chooses one host to write user-defined SQL objects for all the hosts so in fact all those files
/// in the example above are written by single host.

class BackupCoordinationReplicatedSQLObjects
{
public:
    BackupCoordinationReplicatedSQLObjects();
    ~BackupCoordinationReplicatedSQLObjects();

    struct DirectoryPathForSQLObject
    {
        String loader_zk_path;
        UserDefinedSQLObjectType object_type;
        String host_id;
        String dir_path;
    };

    /// Adds a path to directory keeping user defined SQL objects.
    void addDirectory(DirectoryPathForSQLObject && directory_path_for_sql_object);

    /// Returns all added paths to directories if `host_id` is a host chosen to store user-defined SQL objects.
    Strings getDirectories(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & host_id) const;

private:
    using ZkPathAndObjectType = std::pair<String, UserDefinedSQLObjectType>;

    struct DirPathsAndHost
    {
        std::unordered_set<String> dir_paths;
        String host_to_store;
    };

    std::map<ZkPathAndObjectType, DirPathsAndHost> dir_paths_by_zk_path;
};

}
