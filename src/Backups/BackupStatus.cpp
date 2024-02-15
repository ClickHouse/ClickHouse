#include <Backups/BackupStatus.h>
#include <Common/Exception.h>
#include <base/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


std::string_view toString(BackupStatus backup_status)
{
    switch (backup_status)
    {
        case BackupStatus::CREATING_BACKUP:
            return "CREATING_BACKUP";
        case BackupStatus::BACKUP_CREATED:
            return "BACKUP_CREATED";
        case BackupStatus::BACKUP_FAILED:
            return "BACKUP_FAILED";
        case BackupStatus::BACKUP_CANCELLED:
            return "BACKUP_CANCELLED";
        case BackupStatus::RESTORING:
            return "RESTORING";
        case BackupStatus::RESTORED:
            return "RESTORED";
        case BackupStatus::RESTORE_FAILED:
            return "RESTORE_FAILED";
        case BackupStatus::RESTORE_CANCELLED:
            return "RESTORE_CANCELLED";
        default:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected backup status: {}", static_cast<int>(backup_status));
}

const std::vector<std::pair<String, Int8>> & getBackupStatusEnumValues()
{
    static const std::vector<std::pair<String, Int8>> values = []
    {
        std::vector<std::pair<String, Int8>> res;
        for (auto status : collections::range(BackupStatus::MAX))
            res.emplace_back(toString(status), static_cast<Int8>(status));
        return res;
    }();
    return values;
}

}
