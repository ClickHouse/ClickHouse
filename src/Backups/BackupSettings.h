#pragma once

#include <Backups/BackupInfo.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

/// Settings specified in the "SETTINGS" clause of a BACKUP query.
struct BackupSettings
{
    /// ID of the backup operation, to identify it in the system.backups table. Auto-generated if not set.
    String id;

    /// Base backup, if it's set an incremental backup will be built. That means only differences made after the base backup will be put
    /// into a new backup.
    std::optional<BackupInfo> base_backup_info;

    /// Compression method and level for writing the backup (when applicable).
    String compression_method; /// "" means default method
    int compression_level = -1; /// -1 means default level

    /// Password used to encrypt the backup.
    String password;

    /// S3 storage class.
    String s3_storage_class;

    /// If this is set to true then only create queries will be written to backup,
    /// without the data of tables.
    bool structure_only = false;

    /// Whether the BACKUP command must return immediately without waiting until the backup has completed.
    bool async = false;

    /// Whether the BACKUP command should decrypt files stored on encrypted disks.
    bool decrypt_files_from_encrypted_disks = false;

    /// Whether the BACKUP will omit similar files (within one backup only).
    bool deduplicate_files = true;

    /// Whether native copy is allowed (optimization for cloud storages, that sometimes could have bugs)
    bool allow_s3_native_copy = true;

    /// Whether native copy is allowed (optimization for cloud storages, that sometimes could have bugs)
    bool allow_azure_native_copy = true;

    /// Whether base backup to S3 should inherit credentials from the BACKUP query.
    bool use_same_s3_credentials_for_base_backup = false;

    /// Whether base backup archive should be unlocked using the same password as the incremental archive
    bool use_same_password_for_base_backup = false;

    /// Whether a new Azure container should be created if it does not exist (requires permissions at storage account level)
    bool azure_attempt_to_create_container = true;

    /// Allow to use the filesystem cache in passive mode - benefit from the existing cache entries,
    /// but don't put more entries into the cache.
    bool read_from_filesystem_cache = true;

    /// 1-based shard index to store in the backup. 0 means all shards.
    /// Can only be used with BACKUP ON CLUSTER.
    size_t shard_num = 0;

    /// 1-based replica index to store in the backup. 0 means all replicas (see also allow_storing_multiple_replicas).
    /// Can only be used with BACKUP ON CLUSTER.
    size_t replica_num = 0;

    /// Check checksums of the data parts before writing them to a backup.
    bool check_parts = true;

    /// Check checksums of the projection data parts before writing them to a backup.
    bool check_projection_parts = true;

    /// Allow to create backup with broken projections.
    bool allow_backup_broken_projections = false;

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
    /// UUID of the backup. If it's not set it will be generated randomly.
    std::optional<UUID> backup_uuid;

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
