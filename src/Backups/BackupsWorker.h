#pragma once

#include <Backups/BackupStatus.h>
#include <Common/ThreadPool.h>
#include <Core/UUID.h>
#include <Parsers/IAST_fwd.h>
#include <unordered_map>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class ASTBackupQuery;
struct BackupSettings;
struct RestoreSettings;
struct BackupInfo;
class IBackupCoordination;
class IRestoreCoordination;

/// Manager of backups and restores: executes backups and restores' threads in the background.
/// Keeps information about backups and restores started in this session.
class BackupsWorker
{
public:
    BackupsWorker(size_t num_backup_threads, size_t num_restore_threads);

    /// Waits until all tasks have been completed.
    void shutdown();

    /// Backup's or restore's operation ID, can be either passed via SETTINGS id=... or be randomly generated UUID.
    using OperationID = String;

    /// Starts executing a BACKUP or RESTORE query. Returns ID of the operation.
    OperationID start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context);

    /// Waits until a BACKUP or RESTORE query started by start() is finished.
    /// The function returns immediately if the operation is already finished.
    void wait(const OperationID & backup_or_restore_id, bool rethrow_exception = true);

    /// Information about executing a BACKUP or RESTORE query started by calling start().
    struct Info
    {
        /// Backup's or restore's operation ID, can be either passed via SETTINGS id=... or be randomly generated UUID.
        OperationID id;

        /// Backup's name, a string like "Disk('backups', 'my_backup')"
        String name;

        /// Status of backup or restore operation.
        BackupStatus status;

        /// Number of files in the backup (including backup's metadata; only unique files are counted).
        size_t num_files = 0;

        /// Size of all files in the backup (including backup's metadata; only unique files are counted).
        UInt64 uncompressed_size = 0;

        /// Size of the backup if it's stored as an archive; or the same as `uncompressed_size` if the backup is stored as a folder.
        UInt64 compressed_size = 0;

        /// Set only if there was an error.
        std::exception_ptr exception;
        String error_message;

        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
    };

    Info getInfo(const OperationID & id) const;
    std::vector<Info> getAllInfos() const;

private:
    OperationID startMakingBackup(const ASTPtr & query, const ContextPtr & context);

    void doBackup(const std::shared_ptr<ASTBackupQuery> & backup_query, const OperationID & backup_id, BackupSettings backup_settings,
                  const BackupInfo & backup_info, std::shared_ptr<IBackupCoordination> backup_coordination, const ContextPtr & context,
                  ContextMutablePtr mutable_context, bool called_async);

    OperationID startRestoring(const ASTPtr & query, ContextMutablePtr context);

    void doRestore(const std::shared_ptr<ASTBackupQuery> & restore_query, const OperationID & restore_id, const UUID & restore_uuid,
                   RestoreSettings restore_settings, const BackupInfo & backup_info,
                   std::shared_ptr<IRestoreCoordination> restore_coordination, ContextMutablePtr context, bool called_async);

    void addInfo(const OperationID & id, const String & name, BackupStatus status);
    void setStatus(const OperationID & id, BackupStatus status);
    void setNumFilesAndSize(const OperationID & id, size_t num_files, UInt64 uncompressed_size, UInt64 compressed_size);

    ThreadPool backups_thread_pool;
    ThreadPool restores_thread_pool;

    std::unordered_map<OperationID, Info> infos;
    std::condition_variable status_changed;
    std::atomic<size_t> num_active_backups = 0;
    std::atomic<size_t> num_active_restores = 0;
    mutable std::mutex infos_mutex;
    Poco::Logger * log;
};

}
