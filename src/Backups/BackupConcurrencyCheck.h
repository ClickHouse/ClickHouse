#pragma once

#include <Core/UUID.h>
#include <base/scope_guard.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class BackupConcurrencyCounters;

/// Local checker for concurrent BACKUP or RESTORE operations.
/// This class is used by implementations of IBackupCoordination and IRestoreCoordination
/// to throw an exception if concurrent backups or restores are not allowed.
class BackupConcurrencyCheck
{
public:
    /// Checks concurrency of a BACKUP operation or a RESTORE operation.
    /// Keep a constructed instance of BackupConcurrencyCheck until the operation is done.
    BackupConcurrencyCheck(
        const UUID & backup_or_restore_uuid_,
        bool is_restore_,
        bool on_cluster_,
        bool allow_concurrency_,
        BackupConcurrencyCounters & counters_);

    ~BackupConcurrencyCheck();

    [[noreturn]] static void throwConcurrentOperationNotAllowed(bool is_restore);

private:
    const bool is_restore;
    const UUID backup_or_restore_uuid;
    const bool on_cluster;
    BackupConcurrencyCounters & counters;
};


class BackupConcurrencyCounters
{
public:
    BackupConcurrencyCounters();
    ~BackupConcurrencyCounters();

private:
    friend class BackupConcurrencyCheck;
    size_t local_backups TSA_GUARDED_BY(mutex) = 0;
    size_t local_restores TSA_GUARDED_BY(mutex) = 0;
    std::unordered_map<UUID /* backup_uuid */, size_t /* num_refs */> on_cluster_backups TSA_GUARDED_BY(mutex);
    std::unordered_map<UUID /* restore_uuid */, size_t /* num_refs */> on_cluster_restores TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
