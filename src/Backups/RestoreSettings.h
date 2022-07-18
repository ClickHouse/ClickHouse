#pragma once

#include <Backups/BackupInfo.h>
#include <optional>


namespace DB
{
class ASTBackupQuery;

struct StorageRestoreSettings
{
};

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

    /// Whether RESTORE DATABASE must throw an exception if a destination database already exists.
    bool throw_if_database_exists = true;

    /// Whether RESTORE TABLE must throw an exception if a destination table already exists.
    bool throw_if_table_exists = true;

    /// Whether RESTORE DATABASE must throw an exception if a destination database has
    /// a different definition comparing with the definition read from backup.
    bool throw_if_database_def_differs = true;

    /// Whether RESTORE TABLE must throw an exception if a destination table has
    /// a different definition comparing with the definition read from backup.
    bool throw_if_table_def_differs = true;

    static RestoreSettings fromRestoreQuery(const ASTBackupQuery & query);
};

}
