#include <Backups/BackupLocalConcurrencyChecker.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CONCURRENT_ACCESS_NOT_SUPPORTED;
}


BackupLocalConcurrencyChecker::BackupLocalConcurrencyChecker() = default;


BackupLocalConcurrencyChecker::~BackupLocalConcurrencyChecker()
{
    if (local_backups > 0 || local_restores > 0 || !remote_backups.empty() || !remote_restores.empty())
        LOG_ERROR(getLogger(__PRETTY_FUNCTION__), "Some backups or restores are processing");
}


scope_guard BackupLocalConcurrencyChecker::checkLocal(bool is_restore, bool allow_concurrency)
{
    scope_guard res = [this, is_restore]
    {
        std::lock_guard lock{mutex};
        if (is_restore)
            --local_restores;
        else
            --local_backups;
    };

    std::lock_guard lock2{mutex};
    if (is_restore)
        ++local_restores;
    else
        ++local_backups;

    if (!allow_concurrency)
    {
        bool has_concurrent_operation = is_restore ? ((local_restores > 1) || !remote_restores.empty())
                                                   : ((local_backups > 1) || !remote_backups.empty());
        if (has_concurrent_operation)
            throwConcurrentOperationNotAllowed(is_restore);
    }
    return res;
}


scope_guard BackupLocalConcurrencyChecker::checkRemote(bool is_restore, const UUID & backup_or_restore_uuid, bool allow_concurrency)
{
    scope_guard res = [this, is_restore, backup_or_restore_uuid]
    {
        std::lock_guard lock{mutex};
        auto & map = is_restore ? remote_restores : remote_backups;
        auto it = map.find(backup_or_restore_uuid);
        if (it != map.end())
        {
            if (!--it->second)
                map.erase(it);
        }
    };

    std::lock_guard lock2{mutex};
    if (is_restore)
        ++remote_restores[backup_or_restore_uuid];
    else
        ++remote_backups[backup_or_restore_uuid];

    if (!allow_concurrency)
    {
        bool has_concurrent_operation = is_restore ? ((local_restores > 0) || (remote_restores.size() > 1))
                                                   : ((local_backups > 0) || (remote_backups.size() > 1));
        if (has_concurrent_operation)
            throwConcurrentOperationNotAllowed(is_restore);
    }

    return res;
}


void BackupLocalConcurrencyChecker::throwConcurrentOperationNotAllowed(bool is_restore)
{
    throw Exception(
        ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED,
        "Concurrent {} are not allowed, turn on setting '{}'",
        is_restore ? "restores" : "backups",
        is_restore ? "allow_concurrent_restores" : "allow_concurrent_backups");
}

}
