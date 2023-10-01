#pragma once

#include <Backups/BackupOperationInfo.h>
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
class IBackupCoordination;
class IRestoreCoordination;
class IBackup;
using BackupMutablePtr = std::shared_ptr<IBackup>;
using BackupPtr = std::shared_ptr<const IBackup>;
class IBackupEntry;
using BackupEntries = std::vector<std::pair<String, std::shared_ptr<const IBackupEntry>>>;
using DataRestoreTasks = std::vector<std::function<void()>>;
struct ReadSettings;
class BackupLog;

/// Manager of backups and restores: executes backups and restores' threads in the background.
/// Keeps information about backups and restores started in this session.
class BackupsWorker
{
public:
    BackupsWorker(ContextPtr global_context, size_t num_backup_threads, size_t num_restore_threads, bool allow_concurrent_backups_, bool allow_concurrent_restores_);
    ~BackupsWorker();

    /// Waits until all tasks have been completed.
    void shutdown();

    /// Starts executing a BACKUP or RESTORE query. Returns ID of the operation.
    BackupOperationID start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context);

    /// Waits until a BACKUP or RESTORE query started by start() is finished.
    /// The function returns immediately if the operation is already finished.
    void wait(const BackupOperationID & backup_or_restore_id, bool rethrow_exception = true);

    BackupOperationInfo getInfo(const BackupOperationID & id) const;
    std::vector<BackupOperationInfo> getAllInfos() const;

private:
    BackupOperationID startMakingBackup(const ASTPtr & query, const ContextPtr & context);

    void doBackup(
        const std::shared_ptr<ASTBackupQuery> & backup_query,
        const BackupOperationID & backup_id,
        const String & backup_name_for_logging,
        const BackupInfo & backup_info,
        BackupSettings backup_settings,
        std::shared_ptr<IBackupCoordination> backup_coordination,
        const ContextPtr & context,
        ContextMutablePtr mutable_context,
        bool called_async);

    /// Builds file infos for specified backup entries.
    void buildFileInfosForBackupEntries(const BackupPtr & backup, const BackupEntries & backup_entries, const ReadSettings & read_settings, std::shared_ptr<IBackupCoordination> backup_coordination);

    /// Write backup entries to an opened backup.
    void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, const BackupOperationID & backup_id, std::shared_ptr<IBackupCoordination> backup_coordination, bool internal);

    BackupOperationID startRestoring(const ASTPtr & query, ContextMutablePtr context);

    void doRestore(
        const std::shared_ptr<ASTBackupQuery> & restore_query,
        const BackupOperationID & restore_id,
        const String & backup_name_for_logging,
        const BackupInfo & backup_info,
        RestoreSettings restore_settings,
        std::shared_ptr<IRestoreCoordination> restore_coordination,
        ContextMutablePtr context,
        bool called_async);

    /// Run data restoring tasks which insert data to tables.
    void restoreTablesData(const BackupOperationID & restore_id, BackupPtr backup, DataRestoreTasks && tasks, ThreadPool & thread_pool);

    void addInfo(const BackupOperationID & id, const String & name, bool internal, BackupStatus status);
    void setStatus(const BackupOperationID & id, BackupStatus status, bool throw_if_error = true);
    void setStatusSafe(const String & id, BackupStatus status) { setStatus(id, status, false); }
    void setNumFilesAndSize(const BackupOperationID & id, size_t num_files, UInt64 total_size, size_t num_entries,
                            UInt64 uncompressed_size, UInt64 compressed_size, size_t num_read_files, UInt64 num_read_bytes);

    enum class ThreadPoolId;
    ThreadPool & getThreadPool(ThreadPoolId thread_pool_id);

    class ThreadPools;
    std::unique_ptr<ThreadPools> thread_pools;

    const bool allow_concurrent_backups;
    const bool allow_concurrent_restores;
    Poco::Logger * log;

    std::unordered_map<BackupOperationID, BackupOperationInfo> infos;
    std::shared_ptr<BackupLog> backup_log;
    std::condition_variable status_changed;
    std::atomic<size_t> num_active_backups = 0;
    std::atomic<size_t> num_active_restores = 0;
    mutable std::mutex infos_mutex;
};

}
