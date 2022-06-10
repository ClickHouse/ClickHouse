#pragma once

#include <Backups/BackupInfo.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

struct StorageRestoreSettings
{
    /// Internal, should not be specified by user.
    /// The current host's ID in the format 'escaped_host_name:port'.
    String host_id;
};

/// How the RESTORE command will handle table/database existence.
enum class RestoreTableCreationMode
{
    /// RESTORE TABLE always tries to create a table and it throws an exception if the table already exists.
    kCreate,

    /// RESTORE TABLE never tries to create a table and it throws an exception if the table doesn't exist.
    kMustExist,

    /// RESTORE TABLE tries to create a table if it doesn't exist.
    kCreateIfNotExists,
};

using RestoreDatabaseCreationMode = RestoreTableCreationMode;

/// Settings specified in the "SETTINGS" clause of a RESTORE query.
struct RestoreSettings : public StorageRestoreSettings
{
    /// Base backup, with this setting we can override the location of the base backup while restoring.
    /// Any incremental backup keeps inside the information about its base backup, so using this setting is optional.
    std::optional<BackupInfo> base_backup_info;

    /// Password used to decrypt the backup.
    String password;

    /// If this is set to true then only create queries will be read from backup,
    /// without the data of tables.
    bool structure_only = false;

    /// How RESTORE command should work if a table to restore already exists.
    RestoreTableCreationMode create_table = RestoreTableCreationMode::kCreateIfNotExists;

    /// How RESTORE command should work if a database to restore already exists.
    RestoreDatabaseCreationMode create_database = RestoreDatabaseCreationMode::kCreateIfNotExists;

    /// Normally RESTORE command throws an exception if a destination table exists but has a different definition
    /// (i.e. create query) comparing with its definition extracted from backup.
    /// Set `allow_different_table_def` to true to skip this check.
    bool allow_different_table_def = false;

    /// Normally RESTORE command throws an exception if a destination database exists but has a different definition
    /// (i.e. create query) comparing with its definition extracted from backup.
    /// Set `allow_different_database_def` to true to skip this check.
    bool allow_different_database_def = false;

    /// Whether the RESTORE command must return immediately without waiting until the restoring has completed.
    bool async = false;

    /// 1-based shard index to restore from the backup. 0 means all shards.
    /// Can only be used with RESTORE ON CLUSTER.
    size_t shard_num = 0;

    /// 1-based replica index to restore from the backup. 0 means all replicas.
    /// Can only be used with RESTORE ON CLUSTER.
    size_t replica_num = 0;

    /// 1-based index of a shard stored in the backup to get data from.
    /// By default it's 0: if the backup contains only one shard it means the index of that shard
    ///                    else it means the same as `shard`.
    size_t shard_num_in_backup = 0;

    /// 1-based index of a replica stored in the backup to get data from.
    /// By default it's 0: if the backup contains only one replica for the current shard it means the index of that replica
    ///                    else it means the same as `replica`.
    size_t replica_num_in_backup = 0;

    /// Allows RESTORE TABLE to insert data into non-empty tables.
    /// This will mix earlier data in the table with the data extracted from the backup.
    /// Setting "allow_non_empty_tables=true" thus can cause data duplication in the table, use with caution.
    bool allow_non_empty_tables = false;

    /// Internal, should not be specified by user.
    bool internal = false;

    /// Internal, should not be specified by user.
    /// Cluster's hosts' IDs in the format 'escaped_host_name:port' for all shards and replicas in a cluster specified in BACKUP ON CLUSTER.
    std::vector<Strings> cluster_host_ids;

    /// Internal, should not be specified by user.
    /// Path in Zookeeper used to coordinate restoring process while executing by RESTORE ON CLUSTER.
    String coordination_zk_path;

    static RestoreSettings fromRestoreQuery(const ASTBackupQuery & query);
    void copySettingsToQuery(ASTBackupQuery & query) const;
};

}
