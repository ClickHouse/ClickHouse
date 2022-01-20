#pragma once

#include <memory>


namespace DB
{
class ASTBackupQuery;
struct BackupInfo;

/// Settings specified in the "SETTINGS" clause of a BACKUP query.
struct BackupSettings
{
    /// Base backup, if it's set an incremental backup will be built.
    std::shared_ptr<const BackupInfo> base_backup_info;

    /// If this is set to true then only create queries will be written to backup,
    /// without the data of tables.
    bool structure_only = false;

    static BackupSettings fromBackupQuery(const ASTBackupQuery & query);
};

}
