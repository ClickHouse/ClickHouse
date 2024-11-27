#pragma once

#include <base/types.h>


namespace DB
{

enum class BackupStatus : uint8_t
{
    /// Statuses of making backups
    CREATING_BACKUP,
    BACKUP_CREATED,
    BACKUP_FAILED,

    /// Status of restoring
    RESTORING,
    RESTORED,
    RESTORE_FAILED,

    /// Statuses used after a BACKUP or RESTORE operation was cancelled.
    BACKUP_CANCELLED,
    RESTORE_CANCELLED,

    MAX,
};

std::string_view toString(BackupStatus backup_status);

/// Returns vector containing all values of BackupStatus and their string representation,
/// which is used to create DataTypeEnum8 to store those statuses.
const std::vector<std::pair<String, Int8>> & getBackupStatusEnumValues();

}
