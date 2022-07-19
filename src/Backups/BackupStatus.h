#pragma once

#include <base/types.h>


namespace DB
{

enum class BackupStatus
{
    /// Statuses of making backups
    MAKING_BACKUP,
    BACKUP_COMPLETE,
    FAILED_TO_BACKUP,

    /// Status of restoring
    RESTORING,
    RESTORED,
    FAILED_TO_RESTORE,

    MAX,
};

std::string_view toString(BackupStatus backup_status);

/// Returns vector containing all values of BackupStatus and their string representation,
/// which is used to create DataTypeEnum8 to store those statuses.
const std::vector<std::pair<String, Int8>> & getBackupStatusEnumValues();

}
