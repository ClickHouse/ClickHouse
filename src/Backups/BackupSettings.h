#pragma once

#include <Backups/BackupInfo.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

/// Settings specified in the "SETTINGS" clause of a BACKUP query.
struct BackupSettings
{
    /// Base backup, if it's set an incremental backup will be built. That means only differences made after the base backup will be put
    /// into a new backup.
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

    /// Internal, should not be specified by user.
    /// Whether this backup is a part of a distributed backup created by BACKUP ON CLUSTER.
    bool internal = false;

    /// Internal, should not be specified by user.
    /// The current host's ID in the format 'escaped_host_name:port'.
    String host_id;

    /// Internal, should not be specified by user.
    /// Cluster's hosts' IDs in the format 'escaped_host_name:port' for all shards and replicas in a cluster specified in BACKUP ON CLUSTER.
    std::vector<Strings> cluster_host_ids;

    /// Internal, should not be specified by user.
    /// Path in Zookeeper used to coordinate a distributed backup created by BACKUP ON CLUSTER.
    String coordination_zk_path;

    static BackupSettings fromBackupQuery(const ASTBackupQuery & query);
    void copySettingsToQuery(ASTBackupQuery & query) const;

    struct Util
    {
        static std::vector<Strings> clusterHostIDsFromAST(const IAST & ast);
        static ASTPtr clusterHostIDsToAST(const std::vector<Strings> & cluster_host_ids);
        static std::pair<size_t, size_t> findShardNumAndReplicaNum(const std::vector<Strings> & cluster_host_ids, const String & host_id);
        static Strings filterHostIDs(const std::vector<Strings> & cluster_host_ids, size_t only_shard_num, size_t only_replica_num);
    };
};

}
