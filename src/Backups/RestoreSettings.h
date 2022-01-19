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
    std::shared_ptr<const BackupInfo> base_backup_info;
    static RestoreSettings fromRestoreQuery(const ASTBackupQuery & query);
};

}
