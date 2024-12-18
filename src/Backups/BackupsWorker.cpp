#include <Backups/BackupsWorker.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupCoordinationStage.h>
#include <Backups/BackupCoordinationRemote.h>
#include <Backups/BackupCoordinationLocal.h>
#include <Backups/RestoreCoordinationRemote.h>
#include <Backups/RestoreCoordinationLocal.h>
#include <Backups/RestoreSettings.h>
#include <Backups/RestorerFromBackup.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/BackupLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>
#include <Common/scope_guard_safe.h>
#include <Common/ThreadPool.h>
#include <Core/Settings.h>

#include <boost/range/adaptor/map.hpp>


namespace CurrentMetrics
{
    extern const Metric BackupsThreads;
    extern const Metric BackupsThreadsActive;
    extern const Metric BackupsThreadsScheduled;
    extern const Metric RestoreThreads;
    extern const Metric RestoreThreadsActive;
    extern const Metric RestoreThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multiread;
    extern const SettingsUInt64 backup_restore_keeper_max_retries;
    extern const SettingsUInt64 backup_restore_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_fault_injection_seed;
    extern const SettingsFloat backup_restore_keeper_fault_injection_probability;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int CONCURRENT_ACCESS_NOT_SUPPORTED;
    extern const int QUERY_WAS_CANCELLED;
}

using OperationID = BackupOperationID;
namespace Stage = BackupCoordinationStage;

namespace
{
    std::shared_ptr<IBackupCoordination> makeBackupCoordination(const ContextPtr & context, const BackupSettings & backup_settings, bool remote)
    {
        if (remote)
        {
            String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");

            auto get_zookeeper = [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); };

            BackupCoordinationRemote::BackupKeeperSettings keeper_settings = WithRetries::KeeperSettings::fromContext(context);

            auto all_hosts = BackupSettings::Util::filterHostIDs(
                backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num);

            return std::make_shared<BackupCoordinationRemote>(
                get_zookeeper,
                root_zk_path,
                keeper_settings,
                toString(*backup_settings.backup_uuid),
                all_hosts,
                backup_settings.host_id,
                !backup_settings.deduplicate_files,
                backup_settings.internal,
                context->getProcessListElement());
        }

        return std::make_shared<BackupCoordinationLocal>(!backup_settings.deduplicate_files);
    }

    std::shared_ptr<IRestoreCoordination>
    makeRestoreCoordination(const ContextPtr & context, const RestoreSettings & restore_settings, bool remote)
    {
        if (remote)
        {
            String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");

            auto get_zookeeper = [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); };

            RestoreCoordinationRemote::RestoreKeeperSettings keeper_settings
            {
                .keeper_max_retries = context->getSettingsRef()[Setting::backup_restore_keeper_max_retries],
                .keeper_retry_initial_backoff_ms = context->getSettingsRef()[Setting::backup_restore_keeper_retry_initial_backoff_ms],
                .keeper_retry_max_backoff_ms = context->getSettingsRef()[Setting::backup_restore_keeper_retry_max_backoff_ms],
                .batch_size_for_keeper_multiread = context->getSettingsRef()[Setting::backup_restore_batch_size_for_keeper_multiread],
                .keeper_fault_injection_probability = context->getSettingsRef()[Setting::backup_restore_keeper_fault_injection_probability],
                .keeper_fault_injection_seed = context->getSettingsRef()[Setting::backup_restore_keeper_fault_injection_seed]
            };

            auto all_hosts = BackupSettings::Util::filterHostIDs(
                restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num);

            return std::make_shared<RestoreCoordinationRemote>(
                get_zookeeper,
                root_zk_path,
                keeper_settings,
                toString(*restore_settings.restore_uuid),
                all_hosts,
                restore_settings.host_id,
                restore_settings.internal,
                context->getProcessListElement());
        }

        return std::make_shared<RestoreCoordinationLocal>();
    }

    /// Sends information about an exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendExceptionToCoordination(std::shared_ptr<CoordinationType> coordination, const Exception & exception)
    {
        try
        {
            if (coordination)
                coordination->setError(exception);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
    }

    /// Sends information about the current exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendCurrentExceptionToCoordination(std::shared_ptr<CoordinationType> coordination)
    {
        try
        {
            throw;
        }
        catch (const Exception & e)
        {
            sendExceptionToCoordination(coordination, e);
        }
        catch (...)
        {
            sendExceptionToCoordination(coordination, Exception(getCurrentExceptionMessageAndPattern(true, true), getCurrentExceptionCode()));
        }
    }

    bool isFinishedSuccessfully(BackupStatus status)
    {
        return (status == BackupStatus::BACKUP_CREATED) || (status == BackupStatus::RESTORED);
    }

    bool isFailed(BackupStatus status)
    {
        return (status == BackupStatus::BACKUP_FAILED) || (status == BackupStatus::RESTORE_FAILED);
    }

    bool isCancelled(BackupStatus status)
    {
        return (status == BackupStatus::BACKUP_CANCELLED) || (status == BackupStatus::RESTORE_CANCELLED);
    }

    bool isFailedOrCancelled(BackupStatus status)
    {
        return isFailed(status) || isCancelled(status);
    }

    bool isFinalStatus(BackupStatus status)
    {
        return isFinishedSuccessfully(status) || isFailedOrCancelled(status);
    }

    bool isBackupStatus(BackupStatus status)
    {
        return (status == BackupStatus::CREATING_BACKUP) || (status == BackupStatus::BACKUP_CREATED) || (status == BackupStatus::BACKUP_FAILED) || (status == BackupStatus::BACKUP_CANCELLED);
    }

    BackupStatus getBackupStatusFromCurrentException()
    {
        if (getCurrentExceptionCode() == ErrorCodes::QUERY_WAS_CANCELLED)
            return BackupStatus::BACKUP_CANCELLED;
        return BackupStatus::BACKUP_FAILED;
    }

    BackupStatus getRestoreStatusFromCurrentException()
    {
        if (getCurrentExceptionCode() == ErrorCodes::QUERY_WAS_CANCELLED)
            return BackupStatus::RESTORE_CANCELLED;
        return BackupStatus::RESTORE_FAILED;
    }

    /// Used to change num_active_backups.
    size_t getNumActiveBackupsChange(BackupStatus status)
    {
        return status == BackupStatus::CREATING_BACKUP;
    }

    /// Used to change num_active_restores.
    size_t getNumActiveRestoresChange(BackupStatus status)
    {
        return status == BackupStatus::RESTORING;
    }

    /// We use slightly different read and write settings for backup/restore
    /// with a separate throttler and limited usage of filesystem cache.
    ReadSettings getReadSettingsForBackup(const ContextPtr & context, const BackupSettings & backup_settings)
    {
        auto read_settings = context->getReadSettings();
        read_settings.remote_throttler = context->getBackupsThrottler();
        read_settings.local_throttler = context->getBackupsThrottler();
        read_settings.enable_filesystem_cache = backup_settings.read_from_filesystem_cache;
        read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = backup_settings.read_from_filesystem_cache;
        return read_settings;
    }

    WriteSettings getWriteSettingsForBackup(const ContextPtr & context)
    {
        auto write_settings = context->getWriteSettings();
        write_settings.enable_filesystem_cache_on_write_operations = false;
        return write_settings;
    }

    ReadSettings getReadSettingsForRestore(const ContextPtr & context)
    {
        auto read_settings = context->getReadSettings();
        read_settings.remote_throttler = context->getBackupsThrottler();
        read_settings.local_throttler = context->getBackupsThrottler();
        read_settings.enable_filesystem_cache = false;
        read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = false;
        return read_settings;
    }

    WriteSettings getWriteSettingsForRestore(const ContextPtr & context)
    {
        auto write_settings = context->getWriteSettings();
        write_settings.enable_filesystem_cache_on_write_operations = false;
        return write_settings;
    }
}


/// We have to use multiple thread pools because
/// 1) there should be separate thread pools for BACKUP and RESTORE;
/// 2) a task from a thread pool can't wait another task from the same thread pool. (Because if it schedules and waits
/// while the thread pool is still occupied with the waiting task then a scheduled task can be never executed).
enum class BackupsWorker::ThreadPoolId : uint8_t
{
    /// "BACKUP ON CLUSTER ASYNC" waits in background while "BACKUP ASYNC" is finished on the nodes of the cluster, then finalizes the backup.
    BACKUP_ASYNC_ON_CLUSTER = 0,

    /// "BACKUP ASYNC" waits in background while all file infos are built and then it copies the backup's files.
    BACKUP_ASYNC = 1,

    /// Making a list of files to copy and copying of those files is always sequential, so those operations can share one thread pool.
    BACKUP_MAKE_FILES_LIST = 2,
    BACKUP_COPY_FILES = BACKUP_MAKE_FILES_LIST,

    /// "RESTORE ON CLUSTER ASYNC" waits in background while "BACKUP ASYNC" is finished on the nodes of the cluster, then finalizes the backup.
    RESTORE_ASYNC_ON_CLUSTER = 3,

    /// "RESTORE ASYNC" waits in background while the data of all tables are restored.
    RESTORE_ASYNC = 4,

    /// Restores from backups.
    RESTORE = 5,
};


/// Keeps thread pools for BackupsWorker.
class BackupsWorker::ThreadPools
{
public:
    ThreadPools(size_t num_backup_threads_, size_t num_restore_threads_)
        : num_backup_threads(num_backup_threads_), num_restore_threads(num_restore_threads_)
    {
    }

    /// Returns a thread pool, creates it if it's not created yet.
    ThreadPool & getThreadPool(ThreadPoolId thread_pool_id)
    {
        std::lock_guard lock{mutex};
        auto it = thread_pools.find(thread_pool_id);
        if (it != thread_pools.end())
            return *it->second;

        CurrentMetrics::Metric metric_threads;
        CurrentMetrics::Metric metric_active_threads;
        CurrentMetrics::Metric metric_scheduled_threads;
        size_t max_threads = 0;

        /// What to do with a new job if a corresponding thread pool is already running `max_threads` jobs:
        /// `use_queue == true` - put into the thread pool's queue,
        /// `use_queue == false` - schedule() should wait until some of the jobs finish.
        bool use_queue = false;

        switch (thread_pool_id)
        {
            case ThreadPoolId::BACKUP_ASYNC:
            case ThreadPoolId::BACKUP_ASYNC_ON_CLUSTER:
            case ThreadPoolId::BACKUP_COPY_FILES:
            {
                metric_threads = CurrentMetrics::BackupsThreads;
                metric_active_threads = CurrentMetrics::BackupsThreadsActive;
                metric_active_threads = CurrentMetrics::BackupsThreadsScheduled;
                max_threads = num_backup_threads;
                /// We don't use thread pool queues for thread pools with a lot of tasks otherwise that queue could be memory-wasting.
                use_queue = (thread_pool_id != ThreadPoolId::BACKUP_COPY_FILES);
                break;
            }

            case ThreadPoolId::RESTORE_ASYNC:
            case ThreadPoolId::RESTORE_ASYNC_ON_CLUSTER:
            case ThreadPoolId::RESTORE:
            {
                metric_threads = CurrentMetrics::RestoreThreads;
                metric_active_threads = CurrentMetrics::RestoreThreadsActive;
                metric_active_threads = CurrentMetrics::RestoreThreadsScheduled;
                max_threads = num_restore_threads;
                use_queue = true;
                break;
            }
        }

        /// We set max_free_threads = 0 because we don't want to keep any threads if there is no BACKUP or RESTORE query running right now.
        chassert(max_threads != 0);
        size_t max_free_threads = 0;
        size_t queue_size = use_queue ? 0 : max_threads;
        auto thread_pool = std::make_unique<ThreadPool>(metric_threads, metric_active_threads, metric_scheduled_threads, max_threads, max_free_threads, queue_size);
        auto * thread_pool_ptr = thread_pool.get();
        thread_pools.emplace(thread_pool_id, std::move(thread_pool));
        return *thread_pool_ptr;
    }

    /// Waits for all threads to finish.
    void wait()
    {
        auto wait_sequence = {
            ThreadPoolId::RESTORE_ASYNC_ON_CLUSTER,
            ThreadPoolId::RESTORE_ASYNC,
            ThreadPoolId::RESTORE,
            ThreadPoolId::BACKUP_ASYNC_ON_CLUSTER,
            ThreadPoolId::BACKUP_ASYNC,
            ThreadPoolId::BACKUP_COPY_FILES,
        };

        for (auto thread_pool_id : wait_sequence)
        {
            ThreadPool * thread_pool = nullptr;
            {
                std::lock_guard lock{mutex};
                auto it = thread_pools.find(thread_pool_id);
                if (it != thread_pools.end())
                    thread_pool = it->second.get();
            }
            if (thread_pool)
                thread_pool->wait();
        }
    }

private:
    const size_t num_backup_threads;
    const size_t num_restore_threads;
    std::map<ThreadPoolId, std::unique_ptr<ThreadPool>> thread_pools TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};


BackupsWorker::BackupsWorker(ContextMutablePtr global_context, size_t num_backup_threads, size_t num_restore_threads)
    : thread_pools(std::make_unique<ThreadPools>(num_backup_threads, num_restore_threads))
    , allow_concurrent_backups(global_context->getConfigRef().getBool("backups.allow_concurrent_backups", true))
    , allow_concurrent_restores(global_context->getConfigRef().getBool("backups.allow_concurrent_restores", true))
    , remove_backup_files_after_failure(global_context->getConfigRef().getBool("backups.remove_backup_files_after_failure", true))
    , test_randomize_order(global_context->getConfigRef().getBool("backups.test_randomize_order", false))
    , test_inject_sleep(global_context->getConfigRef().getBool("backups.test_inject_sleep", false))
    , log(getLogger("BackupsWorker"))
    , backup_log(global_context->getBackupLog())
    , process_list(global_context->getProcessList())
{
}


BackupsWorker::~BackupsWorker() = default;


ThreadPool & BackupsWorker::getThreadPool(ThreadPoolId thread_pool_id)
{
    return thread_pools->getThreadPool(thread_pool_id);
}


OperationID BackupsWorker::start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context)
{
    const ASTBackupQuery & backup_query = typeid_cast<const ASTBackupQuery &>(*backup_or_restore_query);
    if (backup_query.kind == ASTBackupQuery::Kind::BACKUP)
        return startMakingBackup(backup_or_restore_query, context);
    return startRestoring(backup_or_restore_query, context);
}


OperationID BackupsWorker::startMakingBackup(const ASTPtr & query, const ContextPtr & context)
{
    auto backup_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto backup_settings = BackupSettings::fromBackupQuery(*backup_query);

    auto backup_info = BackupInfo::fromAST(*backup_query->backup_name);
    String backup_name_for_logging = backup_info.toStringForLogging();

    if (!backup_settings.backup_uuid)
        backup_settings.backup_uuid = UUIDHelpers::generateV4();

    /// `backup_id` will be used as a key to the `infos` map, so it should be unique.
    OperationID backup_id;
    if (backup_settings.internal)
        backup_id = "internal-" + toString(UUIDHelpers::generateV4()); /// Always generate `backup_id` for internal backup to avoid collision if both internal and non-internal backups are on the same host
    else if (!backup_settings.id.empty())
        backup_id = backup_settings.id;
    else
        backup_id = toString(*backup_settings.backup_uuid);

    std::shared_ptr<IBackupCoordination> backup_coordination;
    BackupMutablePtr backup;

    /// Called in exception handlers below. This lambda function can be called on a separate thread, so it can't capture local variables by reference.
    auto on_exception = [this](BackupMutablePtr & backup_, const OperationID & backup_id_, const String & backup_name_for_logging_,
                               const BackupSettings & backup_settings_, const std::shared_ptr<IBackupCoordination> & backup_coordination_)
    {
        /// Something bad happened, the backup has not built.
        tryLogCurrentException(log, fmt::format("Failed to make {} {}", (backup_settings_.internal ? "internal backup" : "backup"), backup_name_for_logging_));
        setStatusSafe(backup_id_, getBackupStatusFromCurrentException());
        sendCurrentExceptionToCoordination(backup_coordination_);

        if (backup_ && remove_backup_files_after_failure)
            backup_->tryRemoveAllFiles();
        backup_.reset();
    };

    try
    {
        String base_backup_name;
        if (backup_settings.base_backup_info)
            base_backup_name = backup_settings.base_backup_info->toStringForLogging();

        addInfo(backup_id,
            backup_name_for_logging,
            base_backup_name,
            context->getCurrentQueryId(),
            backup_settings.internal,
            context->getProcessListElement(),
            BackupStatus::CREATING_BACKUP);

        if (backup_settings.internal)
        {
            /// The following call of makeBackupCoordination() is not essential because doBackup() will later create a backup coordination
            /// if it's not created here. However to handle errors better it's better to make a coordination here because this way
            /// if an exception will be thrown in startMakingBackup() other hosts will know about that.
            backup_coordination = makeBackupCoordination(context, backup_settings, /* remote= */ true);
        }

        /// Prepare context to use.
        ContextPtr context_in_use = context;
        ContextMutablePtr mutable_context;
        bool on_cluster = !backup_query->cluster.empty();
        if (on_cluster || backup_settings.async)
        {
            /// We have to clone the query context here because:
            /// if this is an "ON CLUSTER" query we need to change some settings, and
            /// if this is an "ASYNC" query it's going to be executed in another thread.
            context_in_use = mutable_context = Context::createCopy(context);
            mutable_context->makeQueryContext();
        }

        if (backup_settings.async)
        {
            auto & thread_pool = getThreadPool(on_cluster ? ThreadPoolId::BACKUP_ASYNC_ON_CLUSTER : ThreadPoolId::BACKUP_ASYNC);

            /// process_list_element_holder is used to make an element in ProcessList live while BACKUP is working asynchronously.
            auto process_list_element = context_in_use->getProcessListElement();

            thread_pool.scheduleOrThrowOnError(
                [this,
                 backup_query,
                 backup_id,
                 backup_name_for_logging,
                 backup_info,
                 backup_settings,
                 backup_coordination,
                 context_in_use,
                 mutable_context,
                 on_exception,
                 process_list_element_holder = process_list_element ? process_list_element->getProcessListEntry() : nullptr]
                {
                    BackupMutablePtr backup_async;
                    try
                    {
                        setThreadName("BackupWorker");
                        CurrentThread::QueryScope query_scope(context_in_use);
                        doBackup(
                            backup_async,
                            backup_query,
                            backup_id,
                            backup_name_for_logging,
                            backup_info,
                            backup_settings,
                            backup_coordination,
                            context_in_use,
                            mutable_context);
                    }
                    catch (...)
                    {
                        on_exception(backup_async, backup_id, backup_name_for_logging, backup_settings, backup_coordination);
                    }
                });
        }
        else
        {
            doBackup(
                backup,
                backup_query,
                backup_id,
                backup_name_for_logging,
                backup_info,
                backup_settings,
                backup_coordination,
                context_in_use,
                mutable_context);
        }

        return backup_id;
    }
    catch (...)
    {
        on_exception(backup, backup_id, backup_name_for_logging, backup_settings, backup_coordination);
        throw;
    }
}


void BackupsWorker::doBackup(
    BackupMutablePtr & backup,
    const std::shared_ptr<ASTBackupQuery> & backup_query,
    const OperationID & backup_id,
    const String & backup_name_for_logging,
    const BackupInfo & backup_info,
    BackupSettings backup_settings,
    std::shared_ptr<IBackupCoordination> backup_coordination,
    const ContextPtr & context,
    ContextMutablePtr mutable_context)
{
    bool on_cluster = !backup_query->cluster.empty();
    assert(!on_cluster || mutable_context);

    /// Checks access rights if this is not ON CLUSTER query.
    /// (If this is ON CLUSTER query executeDDLQueryOnCluster() will check access rights later.)
    auto required_access = BackupUtils::getRequiredAccessToBackup(backup_query->elements);
    if (!on_cluster)
        context->checkAccess(required_access);

    ClusterPtr cluster;
    if (on_cluster)
    {
        backup_query->cluster = context->getMacros()->expand(backup_query->cluster);
        cluster = context->getCluster(backup_query->cluster);
        backup_settings.cluster_host_ids = cluster->getHostIDs();
    }

    /// Make a backup coordination.
    if (!backup_coordination)
        backup_coordination = makeBackupCoordination(context, backup_settings, /* remote= */ on_cluster);

    if (!allow_concurrent_backups && backup_coordination->hasConcurrentBackups(std::ref(num_active_backups)))
        throw Exception(ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED, "Concurrent backups not supported, turn on setting 'allow_concurrent_backups'");

    /// Opens a backup for writing.
    BackupFactory::CreateParams backup_create_params;
    backup_create_params.open_mode = IBackup::OpenMode::WRITE;
    backup_create_params.context = context;
    backup_create_params.backup_info = backup_info;
    backup_create_params.base_backup_info = backup_settings.base_backup_info;
    backup_create_params.compression_method = backup_settings.compression_method;
    backup_create_params.compression_level = backup_settings.compression_level;
    backup_create_params.password = backup_settings.password;
    backup_create_params.s3_storage_class = backup_settings.s3_storage_class;
    backup_create_params.is_internal_backup = backup_settings.internal;
    backup_create_params.backup_coordination = backup_coordination;
    backup_create_params.backup_uuid = backup_settings.backup_uuid;
    backup_create_params.deduplicate_files = backup_settings.deduplicate_files;
    backup_create_params.allow_s3_native_copy = backup_settings.allow_s3_native_copy;
    backup_create_params.allow_azure_native_copy = backup_settings.allow_azure_native_copy;
    backup_create_params.use_same_s3_credentials_for_base_backup = backup_settings.use_same_s3_credentials_for_base_backup;
    backup_create_params.use_same_password_for_base_backup = backup_settings.use_same_password_for_base_backup;
    backup_create_params.azure_attempt_to_create_container = backup_settings.azure_attempt_to_create_container;
    backup_create_params.read_settings = getReadSettingsForBackup(context, backup_settings);
    backup_create_params.write_settings = getWriteSettingsForBackup(context);
    backup = BackupFactory::instance().createBackup(backup_create_params);

    /// Write the backup.
    if (on_cluster)
    {
        DDLQueryOnClusterParams params;
        params.cluster = cluster;
        params.only_shard_num = backup_settings.shard_num;
        params.only_replica_num = backup_settings.replica_num;
        params.access_to_check = required_access;
        backup_settings.copySettingsToQuery(*backup_query);

        // executeDDLQueryOnCluster() will return without waiting for completion
        mutable_context->setSetting("distributed_ddl_task_timeout", Field{0});
        mutable_context->setSetting("distributed_ddl_output_mode", Field{"none"});
        executeDDLQueryOnCluster(backup_query, mutable_context, params);

        /// Wait until all the hosts have written their backup entries.
        backup_coordination->waitForStage(Stage::COMPLETED);
        backup_coordination->setStage(Stage::COMPLETED,"");
    }
    else
    {
        backup_query->setCurrentDatabase(context->getCurrentDatabase());

        /// Prepare backup entries.
        BackupEntries backup_entries;
        {
            BackupEntriesCollector backup_entries_collector(
                backup_query->elements, backup_settings, backup_coordination,
                backup_create_params.read_settings, context, getThreadPool(ThreadPoolId::BACKUP_MAKE_FILES_LIST));
            backup_entries = backup_entries_collector.run();
        }

        /// Write the backup entries to the backup.
        chassert(backup);
        chassert(backup_coordination);
        chassert(context);
        buildFileInfosForBackupEntries(backup, backup_entries, backup_create_params.read_settings, backup_coordination, context->getProcessListElement());
        writeBackupEntries(backup, std::move(backup_entries), backup_id, backup_coordination, backup_settings.internal, context->getProcessListElement());

        /// We have written our backup entries, we need to tell other hosts (they could be waiting for it).
        backup_coordination->setStage(Stage::COMPLETED,"");
    }

    size_t num_files = 0;
    UInt64 total_size = 0;
    size_t num_entries = 0;
    UInt64 uncompressed_size = 0;
    UInt64 compressed_size = 0;

    /// Finalize backup (write its metadata).
    if (!backup_settings.internal)
    {
        backup->finalizeWriting();
        num_files = backup->getNumFiles();
        total_size = backup->getTotalSize();
        num_entries = backup->getNumEntries();
        uncompressed_size = backup->getUncompressedSize();
        compressed_size = backup->getCompressedSize();
    }

    /// Close the backup.
    backup.reset();

    LOG_INFO(log, "{} {} was created successfully", (backup_settings.internal ? "Internal backup" : "Backup"), backup_name_for_logging);
    /// NOTE: we need to update metadata again after backup->finalizeWriting(), because backup metadata is written there.
    setNumFilesAndSize(backup_id, num_files, total_size, num_entries, uncompressed_size, compressed_size, 0, 0);
    /// NOTE: setStatus is called after setNumFilesAndSize in order to have actual information in a backup log record
    setStatus(backup_id, BackupStatus::BACKUP_CREATED);
}


void BackupsWorker::buildFileInfosForBackupEntries(const BackupPtr & backup, const BackupEntries & backup_entries, const ReadSettings & read_settings, std::shared_ptr<IBackupCoordination> backup_coordination, QueryStatusPtr process_list_element)
{
    backup_coordination->setStage(Stage::BUILDING_FILE_INFOS, "");
    backup_coordination->waitForStage(Stage::BUILDING_FILE_INFOS);
    backup_coordination->addFileInfos(::DB::buildFileInfosForBackupEntries(backup_entries, backup->getBaseBackup(), read_settings, getThreadPool(ThreadPoolId::BACKUP_MAKE_FILES_LIST), process_list_element));
}


void BackupsWorker::writeBackupEntries(
    BackupMutablePtr backup,
    BackupEntries && backup_entries,
    const OperationID & backup_id,
    std::shared_ptr<IBackupCoordination> backup_coordination,
    bool internal,
    QueryStatusPtr process_list_element)
{
    LOG_TRACE(log, "{}, num backup entries={}", Stage::WRITING_BACKUP, backup_entries.size());
    backup_coordination->setStage(Stage::WRITING_BACKUP, "");
    backup_coordination->waitForStage(Stage::WRITING_BACKUP);

    auto file_infos = backup_coordination->getFileInfos();
    if (file_infos.size() != backup_entries.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of file infos ({}) doesn't match the number of backup entries ({})",
            file_infos.size(),
            backup_entries.size());
    }


    std::atomic_bool failed = false;

    bool always_single_threaded = !backup->supportsWritingInMultipleThreads();
    auto & thread_pool = getThreadPool(ThreadPoolId::BACKUP_COPY_FILES);

    std::vector<size_t> writing_order;
    if (test_randomize_order)
    {
        /// Randomize the order in which we write backup entries to the backup.
        writing_order.resize(backup_entries.size());
        std::iota(writing_order.begin(), writing_order.end(), 0);
        std::shuffle(writing_order.begin(), writing_order.end(), thread_local_rng);
    }

    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, "BackupWorker");
    for (size_t i = 0; i != backup_entries.size(); ++i)
    {
        if (failed)
            break;

        size_t index = !writing_order.empty() ? writing_order[i] : i;

        auto & entry = backup_entries[index].second;
        const auto & file_info = file_infos[index];

        auto job = [&]()
        {
            if (failed)
                return;
            try
            {
                if (process_list_element)
                    process_list_element->checkTimeLimit();

                backup->writeFile(file_info, std::move(entry));

                maybeSleepForTesting();

                // Update metadata
                if (!internal)
                {
                    setNumFilesAndSize(
                            backup_id,
                            backup->getNumFiles(),
                            backup->getTotalSize(),
                            backup->getNumEntries(),
                            backup->getUncompressedSize(),
                            backup->getCompressedSize(),
                            0, 0);
                }
            }
            catch (...)
            {
                failed = true;
                throw;
            }
        };

        if (always_single_threaded)
        {
            job();
            continue;
        }

        runner(std::move(job));
    }

    runner.waitForAllToFinishAndRethrowFirstError();
}


OperationID BackupsWorker::startRestoring(const ASTPtr & query, ContextMutablePtr context)
{
    auto restore_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto restore_settings = RestoreSettings::fromRestoreQuery(*restore_query);

    auto backup_info = BackupInfo::fromAST(*restore_query->backup_name);
    String backup_name_for_logging = backup_info.toStringForLogging();

    if (!restore_settings.restore_uuid)
        restore_settings.restore_uuid = UUIDHelpers::generateV4();

    /// `restore_id` will be used as a key to the `infos` map, so it should be unique.
    OperationID restore_id;
    if (restore_settings.internal)
        restore_id = "internal-" + toString(UUIDHelpers::generateV4()); /// Always generate `restore_id` for internal restore to avoid collision if both internal and non-internal restores are on the same host
    else if (!restore_settings.id.empty())
        restore_id = restore_settings.id;
    else
        restore_id = toString(*restore_settings.restore_uuid);

    std::shared_ptr<IRestoreCoordination> restore_coordination;

    /// Called in exception handlers below. This lambda function can be called on a separate thread, so it can't capture local variables by reference.
    auto on_exception = [this](const OperationID & restore_id_, const String & backup_name_for_logging_,
                               const RestoreSettings & restore_settings_, const std::shared_ptr<IRestoreCoordination> & restore_coordination_)
    {
        /// Something bad happened, some data were not restored.
        tryLogCurrentException(log, fmt::format("Failed to restore from {} {}", (restore_settings_.internal ? "internal backup" : "backup"), backup_name_for_logging_));
        setStatusSafe(restore_id_, getRestoreStatusFromCurrentException());
        sendCurrentExceptionToCoordination(restore_coordination_);
    };

    try
    {
        String base_backup_name;
        if (restore_settings.base_backup_info)
            base_backup_name = restore_settings.base_backup_info->toStringForLogging();

        addInfo(restore_id,
            backup_name_for_logging,
            base_backup_name,
            context->getCurrentQueryId(),
            restore_settings.internal,
            context->getProcessListElement(),
            BackupStatus::RESTORING);

        if (restore_settings.internal)
        {
            /// The following call of makeRestoreCoordination() is not essential because doRestore() will later create a restore coordination
            /// if it's not created here. However to handle errors better it's better to make a coordination here because this way
            /// if an exception will be thrown in startRestoring() other hosts will know about that.
            restore_coordination = makeRestoreCoordination(context, restore_settings, /* remote= */ true);
        }

        /// Prepare context to use.
        ContextMutablePtr context_in_use = context;
        bool on_cluster = !restore_query->cluster.empty();
        if (restore_settings.async || on_cluster)
        {
            /// We have to clone the query context here because:
            /// if this is an "ON CLUSTER" query we need to change some settings, and
            /// if this is an "ASYNC" query it's going to be executed in another thread.
            context_in_use = Context::createCopy(context);
            context_in_use->makeQueryContext();
        }

        if (restore_settings.async)
        {
            auto & thread_pool = getThreadPool(on_cluster ? ThreadPoolId::RESTORE_ASYNC_ON_CLUSTER : ThreadPoolId::RESTORE_ASYNC);

            /// process_list_element_holder is used to make an element in ProcessList live while RESTORE is working asynchronously.
            auto process_list_element = context_in_use->getProcessListElement();

            thread_pool.scheduleOrThrowOnError(
                [this,
                 restore_query,
                 restore_id,
                 backup_name_for_logging,
                 backup_info,
                 restore_settings,
                 restore_coordination,
                 context_in_use,
                 on_exception,
                 process_list_element_holder = process_list_element ? process_list_element->getProcessListEntry() : nullptr]
                {
                    try
                    {
                        setThreadName("RestorerWorker");
                        CurrentThread::QueryScope query_scope(context_in_use);
                        doRestore(
                            restore_query,
                            restore_id,
                            backup_name_for_logging,
                            backup_info,
                            restore_settings,
                            restore_coordination,
                            context_in_use);
                    }
                    catch (...)
                    {
                        on_exception(restore_id, backup_name_for_logging, restore_settings, restore_coordination);
                    }
                });
        }
        else
        {
            doRestore(
                restore_query,
                restore_id,
                backup_name_for_logging,
                backup_info,
                restore_settings,
                restore_coordination,
                context_in_use);
        }

        return restore_id;
    }
    catch (...)
    {
        on_exception(restore_id, backup_name_for_logging, restore_settings, restore_coordination);
        throw;
    }
}


void BackupsWorker::doRestore(
    const std::shared_ptr<ASTBackupQuery> & restore_query,
    const OperationID & restore_id,
    const String & backup_name_for_logging,
    const BackupInfo & backup_info,
    RestoreSettings restore_settings,
    std::shared_ptr<IRestoreCoordination> restore_coordination,
    ContextMutablePtr context)
{
    /// Open the backup for reading.
    BackupFactory::CreateParams backup_open_params;
    backup_open_params.open_mode = IBackup::OpenMode::READ;
    backup_open_params.context = context;
    backup_open_params.backup_info = backup_info;
    backup_open_params.base_backup_info = restore_settings.base_backup_info;
    backup_open_params.password = restore_settings.password;
    backup_open_params.allow_s3_native_copy = restore_settings.allow_s3_native_copy;
    backup_open_params.use_same_s3_credentials_for_base_backup = restore_settings.use_same_s3_credentials_for_base_backup;
    backup_open_params.use_same_password_for_base_backup = restore_settings.use_same_password_for_base_backup;
    backup_open_params.read_settings = getReadSettingsForRestore(context);
    backup_open_params.write_settings = getWriteSettingsForRestore(context);
    backup_open_params.is_internal_backup = restore_settings.internal;
    BackupPtr backup = BackupFactory::instance().createBackup(backup_open_params);

    String current_database = context->getCurrentDatabase();
    /// Checks access rights if this is ON CLUSTER query.
    /// (If this isn't ON CLUSTER query RestorerFromBackup will check access rights later.)
    ClusterPtr cluster;
    bool on_cluster = !restore_query->cluster.empty();

    if (on_cluster)
    {
        restore_query->cluster = context->getMacros()->expand(restore_query->cluster);
        cluster = context->getCluster(restore_query->cluster);
        restore_settings.cluster_host_ids = cluster->getHostIDs();
    }

    /// Make a restore coordination.
    if (!restore_coordination)
        restore_coordination = makeRestoreCoordination(context, restore_settings, /* remote= */ on_cluster);

    if (!allow_concurrent_restores && restore_coordination->hasConcurrentRestores(std::ref(num_active_restores)))
        throw Exception(
            ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED,
            "Concurrent restores not supported, turn on setting 'allow_concurrent_restores'");


    if (on_cluster)
    {
        /// We cannot just use access checking provided by the function executeDDLQueryOnCluster(): it would be incorrect
        /// because different replicas can contain different set of tables and so the required access rights can differ too.
        /// So the right way is pass through the entire cluster and check access for each host.
        auto addresses = cluster->filterAddressesByShardOrReplica(restore_settings.shard_num, restore_settings.replica_num);
        for (const auto * address : addresses)
        {
            restore_settings.host_id = address->toString();
            auto restore_elements = restore_query->elements;
            String addr_database = address->default_database.empty() ? current_database : address->default_database;
            for (auto & element : restore_elements)
                element.setCurrentDatabase(addr_database);
            RestorerFromBackup dummy_restorer{restore_elements, restore_settings, nullptr, backup, context, getThreadPool(ThreadPoolId::RESTORE), {}};
            dummy_restorer.run(RestorerFromBackup::CHECK_ACCESS_ONLY);
        }
    }

    /// Do RESTORE.
    if (on_cluster)
    {

        DDLQueryOnClusterParams params;
        params.cluster = cluster;
        params.only_shard_num = restore_settings.shard_num;
        params.only_replica_num = restore_settings.replica_num;
        restore_settings.copySettingsToQuery(*restore_query);

        // executeDDLQueryOnCluster() will return without waiting for completion
        context->setSetting("distributed_ddl_task_timeout", Field{0});
        context->setSetting("distributed_ddl_output_mode", Field{"none"});

        executeDDLQueryOnCluster(restore_query, context, params);

        /// Wait until all the hosts have written their backup entries.
        restore_coordination->waitForStage(Stage::COMPLETED);
        restore_coordination->setStage(Stage::COMPLETED,"");
    }
    else
    {
        restore_query->setCurrentDatabase(current_database);

        auto after_task_callback = [&]
        {
            maybeSleepForTesting();
            setNumFilesAndSize(restore_id, backup->getNumFiles(), backup->getTotalSize(), backup->getNumEntries(),
                               backup->getUncompressedSize(), backup->getCompressedSize(), backup->getNumReadFiles(), backup->getNumReadBytes());
        };

        /// Restore from the backup.
        RestorerFromBackup restorer{restore_query->elements, restore_settings, restore_coordination,
                                    backup, context, getThreadPool(ThreadPoolId::RESTORE), after_task_callback};
        restorer.run(RestorerFromBackup::RESTORE);
    }

    LOG_INFO(log, "Restored from {} {} successfully", (restore_settings.internal ? "internal backup" : "backup"), backup_name_for_logging);
    setStatus(restore_id, BackupStatus::RESTORED);
}


void BackupsWorker::addInfo(const OperationID & id, const String & name, const String & base_backup_name, const String & query_id,
                            bool internal, QueryStatusPtr process_list_element, BackupStatus status)
{
    ExtendedOperationInfo extended_info;
    auto & info = extended_info.info;
    info.id = id;
    info.name = name;
    info.base_backup_name = base_backup_name;
    info.query_id = query_id;
    info.internal = internal;
    info.status = status;
    info.start_time = std::chrono::system_clock::now();

    bool is_final_status = isFinalStatus(status);

    if (process_list_element)
    {
        info.profile_counters = process_list_element->getInfo(/* get_thread_list= */ false, /* get_profile_events= */ true, /* get_settings= */ false).profile_counters;
        if (!is_final_status)
            extended_info.process_list_element = process_list_element;
    }

    if (is_final_status)
        info.end_time = info.start_time;

    std::lock_guard lock{infos_mutex};

    auto it = infos.find(id);
    if (it != infos.end())
    {
        /// It's better not allow to overwrite the current status if it's in progress.
        auto current_status = it->second.info.status;
        if (!isFinalStatus(current_status))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot start a backup or restore: ID {} is already in use", id);
    }

    if (backup_log)
        backup_log->add(BackupLogElement{info});

    infos[id] = std::move(extended_info);

    num_active_backups += getNumActiveBackupsChange(status);
    num_active_restores += getNumActiveRestoresChange(status);
}


void BackupsWorker::setStatus(const String & id, BackupStatus status, bool throw_if_error)
{
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(id);
    if (it == infos.end())
    {
        if (throw_if_error)
           throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", id);
        return;
    }

    auto & extended_info = it->second;
    auto & info = extended_info.info;

    auto old_status = info.status;
    info.status = status;
    bool is_final_status = isFinalStatus(status);

    if (extended_info.process_list_element)
    {
        info.profile_counters = extended_info.process_list_element->getInfo(/* get_thread_list= */ false, /* get_profile_events= */ true, /* get_settings= */ false).profile_counters;
        if (is_final_status)
            extended_info.process_list_element = nullptr;
    }

    if (is_final_status)
        info.end_time = std::chrono::system_clock::now();

    if (isFailedOrCancelled(status))
    {
        info.error_message = getCurrentExceptionMessage(true /*with_stacktrace*/);
        info.exception = std::current_exception();
    }

    if (backup_log)
        backup_log->add(BackupLogElement{info});

    num_active_backups += getNumActiveBackupsChange(status) - getNumActiveBackupsChange(old_status);
    num_active_restores += getNumActiveRestoresChange(status) - getNumActiveRestoresChange(old_status);

    if (status != old_status)
        status_changed.notify_all();
}


void BackupsWorker::setNumFilesAndSize(const OperationID & id, size_t num_files, UInt64 total_size, size_t num_entries,
                                       UInt64 uncompressed_size, UInt64 compressed_size, size_t num_read_files, UInt64 num_read_bytes)

{
    /// Current operation's info entry is updated here. The backup_log table is updated on its basis within a subsequent setStatus() call.
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(id);
    if (it == infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", id);

    auto & info = it->second.info;
    info.num_files = num_files;
    info.total_size = total_size;
    info.num_entries = num_entries;
    info.uncompressed_size = uncompressed_size;
    info.compressed_size = compressed_size;
    info.num_read_files = num_read_files;
    info.num_read_bytes = num_read_bytes;
}


void BackupsWorker::maybeSleepForTesting() const
{
    if (test_inject_sleep)
        sleepForSeconds(1);
}


void BackupsWorker::wait(const OperationID & backup_or_restore_id, bool rethrow_exception)
{
    std::unique_lock lock{infos_mutex};
    status_changed.wait(lock, [&]
    {
        auto it = infos.find(backup_or_restore_id);
        if (it == infos.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", backup_or_restore_id);
        const auto & info = it->second.info;
        auto current_status = info.status;
        if (rethrow_exception && isFailedOrCancelled(current_status))
            std::rethrow_exception(info.exception);
        if (isFinalStatus(current_status))
            return true;
        LOG_INFO(log, "Waiting {} {}", isBackupStatus(info.status) ? "backup" : "restore", info.name);
        return false;
    });
}

void BackupsWorker::waitAll()
{
    std::vector<OperationID> current_operations;
    {
        std::lock_guard lock{infos_mutex};
        for (const auto & [id, extended_info] : infos)
            if (!isFinalStatus(extended_info.info.status))
                current_operations.push_back(id);
    }

    if (current_operations.empty())
        return;

    LOG_INFO(log, "Waiting for running backups and restores to finish");

    for (const auto & id : current_operations)
        wait(id, /* rethrow_exception= */ false);

    LOG_INFO(log, "Backups and restores finished");
}

void BackupsWorker::cancel(const BackupOperationID & backup_or_restore_id, bool wait_)
{
    QueryStatusPtr process_list_element;
    {
        std::unique_lock lock{infos_mutex};
        auto it = infos.find(backup_or_restore_id);
        if (it == infos.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", backup_or_restore_id);

        const auto & extended_info = it->second;
        const auto & info = extended_info.info;
        if (isFinalStatus(info.status) || !extended_info.process_list_element)
            return;

        LOG_INFO(log, "Cancelling {} {}", isBackupStatus(info.status) ? "backup" : "restore", info.name);
        process_list_element = extended_info.process_list_element;
    }

    process_list.sendCancelToQuery(process_list_element);

    if (wait_)
        wait(backup_or_restore_id, /* rethrow_exception= */ false);
}


void BackupsWorker::cancelAll(bool wait_)
{
    std::vector<OperationID> current_operations;
    {
        std::lock_guard lock{infos_mutex};
        for (const auto & [id, extended_info] : infos)
            if (!isFinalStatus(extended_info.info.status))
                current_operations.push_back(id);
    }

    if (current_operations.empty())
        return;

    LOG_INFO(log, "Cancelling running backups and restores");

    for (const auto & id : current_operations)
        cancel(id, /* wait= */ false);

    if (wait_)
        for (const auto & id : current_operations)
            wait(id, /* rethrow_exception= */ false);

    LOG_INFO(log, "Backups and restores finished or stopped");
}


BackupOperationInfo BackupsWorker::getInfo(const OperationID & id) const
{
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(id);
    if (it == infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", id);
    return it->second.info;
}

std::vector<BackupOperationInfo> BackupsWorker::getAllInfos() const
{
    std::vector<BackupOperationInfo> res_infos;
    std::lock_guard lock{infos_mutex};
    for (const auto & extended_info : infos | boost::adaptors::map_values)
    {
        const auto & info = extended_info.info;
        if (!info.internal)
            res_infos.push_back(info);
    }
    return res_infos;
}

void BackupsWorker::shutdown()
{
    /// Cancel running backups and restores.
    cancelAll(/* wait= */ true);

    /// Wait for our thread pools (it must be done before destroying them).
    thread_pools->wait();
}

}
