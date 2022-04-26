#pragma once

#include <Backups/BackupInfo.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

/// Settings specified in the "SETTINGS" clause of a BACKUP query.
struct BackupSettings
{
    /// Base backup, if it's set an incremental backup will be built.
    std::optional<BackupInfo> base_backup_info;

    /// Compression method and level for writing the backup (when applicable).
    String compression_method; /// "" means default method
    int compression_level = -1; /// -1 means default level

    /// Password used to encrypt the backup.
    String password;

    /// If this is set to true then only create queries will be written to backup,
    /// without the data of tables.
    bool structure_only = false;

    /// Whether the BACKUP command must return immediately without waiting until the backup has completed.
    bool async = false;

    /// 1-based shard index to store in the backup. 0 means all shards.
    /// Can only be used with BACKUP ON CLUSTER.
    size_t shard_num = 0;

    /// 1-based replica index to store in the backup. 0 means all replicas (see also allow_storing_multiple_replicas).
    /// Can only be used with BACKUP ON CLUSTER.
    size_t replica_num = 0;

    /// Allows storing in the backup of multiple replicas.
    bool allow_storing_multiple_replicas = false;

    /// Internal, should not be specified by user.
    /// Whether this backup is a part of a distributed backup created by BACKUP ON CLUSTER.
    bool internal = false;

    /// Internal, should not be specified by user.
    /// Path in Zookeeper used to coordinate a distributed backup created by BACKUP ON CLUSTER.
    String coordination_zk_path;

    static BackupSettings fromBackupQuery(const ASTBackupQuery & query);
    void copySettingsToBackupQuery(ASTBackupQuery & query) const;
};

}
