#pragma once

#include <Backups/BackupStatus.h>
#include <Backups/BackupRestoreCleanupThread.h>
#include <Common/ThreadPool_fwd.h>
#include <Interpreters/Context_fwd.h>
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
struct Settings;
class IBackupCoordination;
class IRestoreCoordination;
class IBackup;
using BackupMutablePtr = std::shared_ptr<IBackup>;
using BackupPtr = std::shared_ptr<const IBackup>;
class IBackupEntry;
using BackupEntries = std::vector<std::pair<String, std::shared_ptr<const IBackupEntry>>>;
using DataRestoreTasks = std::vector<std::function<void()>>;

/// Manager of backups and restores: executes backups and restores' threads in the background.
/// Keeps information about backups and restores started in this session.
class BackupsWorker
{
public:
    struct GlobalSettings
    {
        size_t num_backup_threads;
        size_t num_restore_threads;
        bool allow_concurrent_backups;
        bool allow_concurrent_restores;
        size_t stale_backups_restores_check_period_ms;
        size_t stale_backups_restores_cleanup_timeout_ms;
        size_t consecutive_failed_checks_to_consider_as_stale;
        String root_zookeeper_path;
    };

    explicit BackupsWorker(const GlobalSettings & global_backup_settings);

    static std::unique_ptr<BackupsWorker> createFromContext(ContextPtr global_context);

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

        /// This operation is internal and should not be shown in system.backups
        bool internal = false;

        /// Status of backup or restore operation.
        BackupStatus status;

        /// The number of files stored in the backup.
        size_t num_files = 0;

        /// The total size of files stored in the backup.
        UInt64 total_size = 0;

        /// The number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder.
        size_t num_entries = 0;

        /// The uncompressed size of the backup.
        UInt64 uncompressed_size = 0;

        /// The compressed size of the backup.
        UInt64 compressed_size = 0;

        /// Returns the number of files read during RESTORE from this backup.
        size_t num_read_files = 0;

        // Returns the total size of files read during RESTORE from this backup.
        UInt64 num_read_bytes = 0;

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

    void doBackup(
        const std::shared_ptr<ASTBackupQuery> & backup_query,
        const OperationID & backup_id,
        const String & backup_name_for_logging,
        const BackupInfo & backup_info,
        BackupSettings backup_settings,
        std::shared_ptr<IBackupCoordination> backup_coordination,
        const ContextPtr & context,
        ContextMutablePtr mutable_context,
        bool called_async);

    /// Builds file infos for specified backup entries.
    void buildFileInfosForBackupEntries(const BackupPtr & backup, const BackupEntries & backup_entries, std::shared_ptr<IBackupCoordination> backup_coordination);

    /// Write backup entries to an opened backup.
    void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, const OperationID & backup_id, std::shared_ptr<IBackupCoordination> backup_coordination, bool internal);

    OperationID startRestoring(const ASTPtr & query, ContextMutablePtr context);

    void doRestore(
        const std::shared_ptr<ASTBackupQuery> & restore_query,
        const OperationID & restore_id,
        const String & backup_name_for_logging,
        const BackupInfo & backup_info,
        RestoreSettings restore_settings,
        std::shared_ptr<IRestoreCoordination> restore_coordination,
        ContextMutablePtr context,
        bool called_async);

    /// Run data restoring tasks which insert data to tables.
    void restoreTablesData(const OperationID & restore_id, BackupPtr backup, DataRestoreTasks && tasks, ThreadPool & thread_pool);

    void addInfo(const OperationID & id, const String & name, bool internal, BackupStatus status);
    void setStatus(const OperationID & id, BackupStatus status, bool throw_if_error = true);
    void setStatusSafe(const String & id, BackupStatus status) { setStatus(id, status, false); }
    void setNumFilesAndSize(const OperationID & id, size_t num_files, UInt64 total_size, size_t num_entries,
                            UInt64 uncompressed_size, UInt64 compressed_size, size_t num_read_files, UInt64 num_read_bytes);

    GlobalSettings global_settings;
    std::unique_ptr<ThreadPool> backups_thread_pool;
    std::unique_ptr<ThreadPool> restores_thread_pool;

    std::unordered_map<OperationID, Info> infos;
    std::condition_variable status_changed;
    std::atomic<size_t> num_active_backups = 0;
    std::atomic<size_t> num_active_restores = 0;
    mutable std::mutex infos_mutex;
    Poco::Logger * log;
    const bool allow_concurrent_backups;
    const bool allow_concurrent_restores;
    std::optional<BackupRestoreCleanupThread> backup_restore_cleanup_thread;
};

}
