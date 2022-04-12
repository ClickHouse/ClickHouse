#pragma once

#include <Backups/BackupInfo.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

struct StorageRestoreSettings
{
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
    /// Any incremental backup keeps inside the information about its base backup,
    /// so using this setting is optional.
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

    static RestoreSettings fromRestoreQuery(const ASTBackupQuery & query);
};

}
