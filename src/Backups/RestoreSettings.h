#pragma once

#include <Backups/BackupInfo.h>
#include <Common/SettingsChanges.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

/// How the RESTORE command will handle table/database existence.
enum class RestoreTableCreationMode : uint8_t
{
    /// RESTORE TABLE always tries to create a table and it throws an exception if the table already exists.
    kCreate,

    /// RESTORE TABLE never tries to create a table and it throws an exception if the table doesn't exist.
    kMustExist,

    /// RESTORE TABLE tries to create a table if it doesn't exist.
    kCreateIfNotExists,
};

using RestoreDatabaseCreationMode = RestoreTableCreationMode;

/// How the RESTORE command will handle if an user (or role or profile) which it's going to restore already exists.
enum class RestoreAccessCreationMode : uint8_t
{
    /// RESTORE will throw an exception if some user already exists.
    kCreate,

    /// RESTORE will skip existing users.
    kCreateIfNotExists,

    /// RESTORE will replace existing users with definitions from backup.
    kReplace,
};

using RestoreUDFCreationMode = RestoreAccessCreationMode;

/// Settings specified in the "SETTINGS" clause of a RESTORE query.
struct RestoreSettings
{
    /// ID of the restore operation, to identify it in the system.backups table. Auto-generated if not set.
    String id;

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

    /// How the RESTORE command will handle if an user (or role or profile) which it's going to restore already exists.
    RestoreAccessCreationMode create_access = RestoreAccessCreationMode::kCreateIfNotExists;

    /// Ignore dependencies or dependents (with update_access_entities_dependents=true) of access entities in the case if they can't be resolved.
    /// For example: if a backup contains a profile assigned to a user: `CREATE PROFILE p1; CREATE USER u1 SETTINGS PROFILE p1`
    /// and now we're restoring only user `u1` and profile `p1` doesn't exists, then
    /// this flag is whether RESTORE should continue with restoring user `u1` without assigning profile `p1`.
    /// Another example: if a backup contains a role granted to a user: `CREATE USER u2; CREATE ROLE r2; GRANT r2 TO u2`
    /// and now we're restoring only user `u2` and role `r2` doesn't exist, then
    /// this flag is whether RESTORE should continue with restoring user `u2` without that grant.
    /// If this flag is false then RESTORE will throw an exception in that case.
    bool skip_unresolved_access_dependencies = false;

    /// Try to update dependents of restored access entities.
    /// For example: if a backup contains a profile assigned to a user: `CREATE PROFILE p1; CREATE USER u1 SETTINGS PROFILE p1`
    /// and now we're restoring only profile `p1` and user `u1` already exists, then
    /// this flag is whether restored profile `p1` should be assigned to user `u1` again.
    /// Another example, if a backup contains a role granted to a user: `CREATE USER u2; CREATE ROLE r2; GRANT r2 TO u2`
    /// and now we're restoring only role `r2` and user `u2` already exists, then
    /// this flag is whether restored role `r2` should be granted to user `u2` again.
    /// If this flag is false then RESTORE won't update existing access entities.
    bool update_access_entities_dependents = true;

    /// How the RESTORE command will handle if a user-defined function which it's going to restore already exists.
    RestoreUDFCreationMode create_function = RestoreUDFCreationMode::kCreateIfNotExists;

    /// Whether native copy is allowed (optimization for cloud storages, that sometimes could have bugs)
    bool allow_s3_native_copy = true;

    /// Whether native copy is allowed for AzureBlobStorage
    bool allow_azure_native_copy = true;

    /// Whether base backup from S3 should inherit credentials from the RESTORE query.
    bool use_same_s3_credentials_for_base_backup = false;

    /// Whether base backup archive should be unlocked using the same password as the incremental archive
    bool use_same_password_for_base_backup = false;

    /// If it's true RESTORE won't stop on broken parts while restoring, instead they will be restored as detached parts
    /// to the `detached` folder with names starting with `broken-from-backup'.
    bool restore_broken_parts_as_detached = false;

    /// Internal, should not be specified by user.
    bool internal = false;

    /// Internal, should not be specified by user.
    /// The current host's ID in the format 'escaped_host_name:port'.
    String host_id;

    /// Alternative storage policy that may be specified in the SETTINGS clause of RESTORE queries
    std::optional<String> storage_policy;

    /// Internal, should not be specified by user.
    /// Cluster's hosts' IDs in the format 'escaped_host_name:port' for all shards and replicas in a cluster specified in BACKUP ON CLUSTER.
    std::vector<Strings> cluster_host_ids;

    /// Internal, should not be specified by user.
    /// UUID of the restore. If it's not set it will be generated randomly.
    /// This is used to generate coordination path and for concurrency check
    std::optional<UUID> restore_uuid;

    /// Core settings specified in the query.
    SettingsChanges core_settings;

    static RestoreSettings fromRestoreQuery(const ASTBackupQuery & query);
    void copySettingsToQuery(ASTBackupQuery & query) const;
};

}
