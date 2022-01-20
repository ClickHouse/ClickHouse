#pragma once

#include <memory>


namespace DB
{
class ASTBackupQuery;
struct BackupInfo;

struct StorageRestoreSettings
{
};

/// Settings specified in the "SETTINGS" clause of a RESTORE query.
struct RestoreSettings : public StorageRestoreSettings
{
    /// Base backup, with this setting we can override the location of the base backup while restoring.
    /// Any incremental backup keeps inside the information about its base backup,
    /// so using this setting is optional.
    std::shared_ptr<const BackupInfo> base_backup_info;

    /// If this is set to true then only create queries will be read from backup,
    /// without the data of tables.
    bool structure_only = false;

    static RestoreSettings fromRestoreQuery(const ASTBackupQuery & query);
};

}
