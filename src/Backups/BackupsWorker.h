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
class BackupConcurrencyCounters;
using DataRestoreTasks = std::vector<std::function<void()>>;
struct ReadSettings;
class BackupLog;
class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ProcessList;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;
class AccessRightsElements;
struct ZooKeeperRetriesInfo;


/// Manager of backups and restores: executes backups and restores' threads in the background.
/// Keeps information about backups and restores started in this session.
class BackupsWorker
{
public:
    BackupsWorker(ContextMutablePtr global_context, size_t num_backup_threads, size_t num_restore_threads);
    ~BackupsWorker();

    /// Waits until all tasks have been completed.
    void shutdown();

    /// Starts executing a BACKUP or RESTORE query. Returns ID of the operation.
    /// For asynchronous operations the function throws no exceptions on failure usually,
    /// call getInfo() on a returned operation id to check for errors.
    std::pair<BackupOperationID, BackupStatus> start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context);

    /// Waits until the specified backup or restore operation finishes or stops.
    /// The function returns immediately if the operation is already finished.
    BackupStatus wait(const BackupOperationID & backup_or_restore_id, bool rethrow_exception = true);

    /// Waits until all running backup and restore operations finish or stop.
    void waitAll();

    /// Cancels the specified backup or restore operation.
    /// The function does nothing if this operation has already finished.
    BackupStatus cancel(const BackupOperationID & backup_or_restore_id, bool wait_ = true);

    /// Cancels all running backup and restore operations.
    void cancelAll(bool wait_ = true);

    BackupOperationInfo getInfo(const BackupOperationID & id) const;
    std::vector<BackupOperationInfo> getAllInfos() const;

private:
    std::pair<BackupOperationID, BackupStatus> startMakingBackup(const ASTPtr & query, const ContextPtr & context);
    struct BackupStarter;

    BackupMutablePtr openBackupForWriting(const BackupInfo & backup_info, const BackupSettings & backup_settings, std::shared_ptr<IBackupCoordination> backup_coordination, const ContextPtr & context) const;

    void doBackup(
        BackupMutablePtr backup,
        const std::shared_ptr<ASTBackupQuery> & backup_query,
        const BackupOperationID & backup_id,
        const String & backup_name_for_logging,
        const BackupSettings & backup_settings,
        std::shared_ptr<IBackupCoordination> backup_coordination,
        ContextMutablePtr context,
        bool on_cluster,
        const ClusterPtr & cluster);

    /// Builds file infos for specified backup entries.
    void buildFileInfosForBackupEntries(const BackupPtr & backup, const BackupEntries & backup_entries, const ReadSettings & read_settings, std::shared_ptr<IBackupCoordination> backup_coordination, QueryStatusPtr process_list_element);

    /// Write backup entries to an opened backup.
    void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, const BackupOperationID & backup_id, std::shared_ptr<IBackupCoordination> backup_coordination, bool is_internal_backup, QueryStatusPtr process_list_element);

    std::pair<BackupOperationID, BackupStatus> startRestoring(const ASTPtr & query, ContextMutablePtr context);
    struct RestoreStarter;

    BackupPtr openBackupForReading(const BackupInfo & backup_info, const RestoreSettings & restore_settings, const ContextPtr & context) const;

    void doRestore(
        const std::shared_ptr<ASTBackupQuery> & restore_query,
        const BackupOperationID & restore_id,
        const String & backup_name_for_logging,
        const BackupInfo & backup_info,
        RestoreSettings restore_settings,
        std::shared_ptr<IRestoreCoordination> restore_coordination,
        ContextMutablePtr context,
        bool on_cluster,
        const ClusterPtr & cluster);

    std::shared_ptr<IBackupCoordination> makeBackupCoordination(bool on_cluster, const BackupSettings & backup_settings, const ContextPtr & context) const;
    std::shared_ptr<IRestoreCoordination> makeRestoreCoordination(bool on_cluster, const RestoreSettings & restore_settings, const ContextPtr & context) const;

    /// Sends a BACKUP or RESTORE query to other hosts.
    void sendQueryToOtherHosts(const ASTBackupQuery & backup_or_restore_query, const ClusterPtr & cluster,
        size_t only_shard_num, size_t only_replica_num, ContextMutablePtr context, const AccessRightsElements & access_to_check,
        const ZooKeeperRetriesInfo & retries_info) const;

    /// Run data restoring tasks which insert data to tables.
    void restoreTablesData(const BackupOperationID & restore_id, BackupPtr backup, DataRestoreTasks && tasks, ThreadPool & thread_pool, QueryStatusPtr process_list_element);

    void addInfo(const BackupOperationID & id, const String & name, const String & base_backup_name, const String & query_id,
                bool internal, QueryStatusPtr process_list_element, BackupStatus status);
    void setStatus(const BackupOperationID & id, BackupStatus status, bool throw_if_error = true);
    void setStatusSafe(const String & id, BackupStatus status) { setStatus(id, status, false); }
    void setNumFilesAndSize(const BackupOperationID & id, size_t num_files, UInt64 total_size, size_t num_entries,
                            UInt64 uncompressed_size, UInt64 compressed_size, size_t num_read_files, UInt64 num_read_bytes);

    enum class ThreadPoolId : uint8_t;
    ThreadPool & getThreadPool(ThreadPoolId thread_pool_id);

    /// Waits for some time if `test_inject_sleep` is true.
    void maybeSleepForTesting() const;

    class ThreadPools;
    std::unique_ptr<ThreadPools> thread_pools;

    const bool allow_concurrent_backups;
    const bool allow_concurrent_restores;
    const bool remove_backup_files_after_failure;
    const bool test_randomize_order;
    const bool test_inject_sleep;

    LoggerPtr log;

    struct ExtendedOperationInfo
    {
        BackupOperationInfo info;
        QueryStatusPtr process_list_element; /// to cancel this operation if we want to
    };

    std::unordered_map<BackupOperationID, ExtendedOperationInfo> infos;

    std::condition_variable status_changed;
    std::atomic<size_t> num_active_backups = 0;
    std::atomic<size_t> num_active_restores = 0;
    mutable std::mutex infos_mutex;

    std::shared_ptr<BackupLog> backup_log;
    ProcessList & process_list;

    std::unique_ptr<BackupConcurrencyCounters> concurrency_counters;
};

}
