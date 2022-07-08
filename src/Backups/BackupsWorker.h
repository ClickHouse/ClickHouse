#pragma once

#include <Backups/BackupStatus.h>
#include <Common/ThreadPool.h>
#include <Core/UUID.h>
#include <Parsers/IAST_fwd.h>
#include <unordered_map>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
/// Manager of backups and restores: executes backups and restores' threads in the background.
/// Keeps information about backups and restores started in this session.
class BackupsWorker
{
public:
    BackupsWorker(size_t num_backup_threads, size_t num_restore_threads);

    /// Waits until all tasks have been completed.
    void shutdown();

    /// Starts executing a BACKUP or RESTORE query. Returns UUID of the operation.
    UUID start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context);

    /// Waits until a BACKUP or RESTORE query started by start() is finished.
    /// The function returns immediately if the operation is already finished.
    void wait(const UUID & backup_or_restore_uuid, bool rethrow_exception = true);

    /// Information about executing a BACKUP or RESTORE query started by calling start().
    struct Info
    {
        UUID uuid;

        /// Backup's name, a string like "Disk('backups', 'my_backup')"
        String backup_name;

        BackupStatus status;
        time_t status_changed_time;

        String error_message;
        std::exception_ptr exception;

        /// Whether this operation is internal, i.e. caused by another BACKUP or RESTORE operation.
        /// For example BACKUP ON CLUSTER executes an internal BACKUP commands per each node.
        bool internal = false;
    };

    Info getInfo(const UUID & backup_or_restore_uuid) const;
    std::vector<Info> getAllInfos() const;

private:
    UUID startMakingBackup(const ASTPtr & query, const ContextPtr & context);
    UUID startRestoring(const ASTPtr & query, ContextMutablePtr context);

    void addInfo(const UUID & uuid, const String & backup_name, BackupStatus status, bool internal);
    void setStatus(const UUID & uuid, BackupStatus status);

    ThreadPool backups_thread_pool;
    ThreadPool restores_thread_pool;

    std::unordered_map<UUID, Info> infos;
    std::condition_variable status_changed;
    mutable std::mutex infos_mutex;
    Poco::Logger * log;
};

}
