#pragma once

#include <Core/UUID.h>
#include <base/scope_guard.h>
#include <mutex>
#include <unordered_map>


namespace DB
{

/// Local checker for concurrent backup or restore operations.
class BackupLocalConcurrencyChecker
{
public:
    BackupLocalConcurrencyChecker();
    ~BackupLocalConcurrencyChecker();

    scope_guard checkLocal(bool is_restore, bool allow_concurrency);
    scope_guard checkRemote(bool is_restore, const UUID & backup_or_restore_uuid, bool allow_concurrency);

    [[noreturn]] static void throwConcurrentOperationNotAllowed(bool is_restore);

private:
    size_t local_backups TSA_GUARDED_BY(mutex) = 0;
    size_t local_restores TSA_GUARDED_BY(mutex) = 0;
    std::unordered_map<UUID /* backup_uuid */, size_t /* num_refs */> remote_backups TSA_GUARDED_BY(mutex);
    std::unordered_map<UUID /* restore_uuid */, size_t /* num_refs */> remote_restores TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
