#pragma once

#include <memory>


namespace DB
{
class ASTBackupQuery;
struct BackupInfo;

/// Settings specified in the "SETTINGS" clause of a BACKUP query.
struct BackupSettings
{
    std::shared_ptr<const BackupInfo> base_backup_info;
    static BackupSettings fromBackupQuery(const ASTBackupQuery & query);
};

}
