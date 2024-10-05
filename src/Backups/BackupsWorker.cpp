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

#include <Processors/Executors/CompletedPipelineExecutor.h>

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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

using OperationID = BackupOperationID;
namespace Stage = BackupCoordinationStage;

namespace
{
    /// Sends information about an exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendExceptionToCoordination(std::shared_ptr<CoordinationType> coordination, const Exception & exception) noexcept
    {
        try
        {
            coordination->setError(exception);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
    }

    /// Sends information about the current exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendCurrentExceptionToCoordination(std::shared_ptr<CoordinationType> coordination) noexcept
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
        else
            return BackupStatus::BACKUP_FAILED;
    }

    BackupStatus getRestoreStatusFromCurrentException()
    {
        if (getCurrentExceptionCode() == ErrorCodes::QUERY_WAS_CANCELLED)
            return BackupStatus::RESTORE_CANCELLED;
        else
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

    BACKUP_COORDINATION_WORKER_ON_CLUSTER = 1,

    /// "BACKUP ASYNC" waits in background while all file infos are built and then it copies the backup's files.
    BACKUP_ASYNC = 2,

    BACKUP_COORDINATION_WORKER = 3,

    /// Making a list of files to copy and copying of those files is always sequential, so those operations can share one thread pool.
    BACKUP_MAKE_FILES_LIST = 4,
    BACKUP_COPY_FILES = BACKUP_MAKE_FILES_LIST,

    /// "RESTORE ON CLUSTER ASYNC" waits in background while "BACKUP ASYNC" is finished on the nodes of the cluster, then finalizes the backup.
    RESTORE_ASYNC_ON_CLUSTER = 5,

    RESTORE_COORDINATION_WORKER_ON_CLUSTER = 6,

    /// "RESTORE ASYNC" waits in background while the data of all tables are restored.
    RESTORE_ASYNC = 7,

    RESTORE_COORDINATION_WORKER = 8,

    /// Restores from backups.
    RESTORE = 9,
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
            case ThreadPoolId::BACKUP_COORDINATION_WORKER:
            case ThreadPoolId::BACKUP_COORDINATION_WORKER_ON_CLUSTER:
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
            case ThreadPoolId::RESTORE_COORDINATION_WORKER:
            case ThreadPoolId::RESTORE_COORDINATION_WORKER_ON_CLUSTER:
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
            ThreadPoolId::RESTORE_COORDINATION_WORKER_ON_CLUSTER,
            ThreadPoolId::RESTORE_ASYNC,
            ThreadPoolId::RESTORE_COORDINATION_WORKER,
            ThreadPoolId::RESTORE,
            ThreadPoolId::BACKUP_ASYNC_ON_CLUSTER,
            ThreadPoolId::BACKUP_COORDINATION_WORKER_ON_CLUSTER,
            ThreadPoolId::BACKUP_ASYNC,
            ThreadPoolId::BACKUP_COORDINATION_WORKER,
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
    , concurrency_checker(std::make_unique<BackupLocalConcurrencyChecker>())
{
}


BackupsWorker::~BackupsWorker() = default;


ThreadPool & BackupsWorker::getThreadPool(ThreadPoolId thread_pool_id)
{
    return thread_pools->getThreadPool(thread_pool_id);
}


std::pair<OperationID, BackupStatus> BackupsWorker::start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context)
{
    const ASTBackupQuery & backup_query = typeid_cast<const ASTBackupQuery &>(*backup_or_restore_query);
    if (backup_query.kind == ASTBackupQuery::Kind::BACKUP)
        return startMakingBackup(backup_or_restore_query, context);
    else
        return startRestoring(backup_or_restore_query, context);
}


struct BackupsWorker::BackupStarter
{
    BackupsWorker & backups_worker;
    std::shared_ptr<ASTBackupQuery> backup_query;
    ContextPtr query_context; /// We have to keep `query_context` until the end of the operation because a pointer to it is stored inside the ThreadGroup we're using.
    ContextMutablePtr backup_context;
    BackupSettings backup_settings;
    BackupInfo backup_info;
    String backup_id;
    String backup_name_for_logging;
    bool on_cluster;
    bool is_internal_backup;
    std::shared_ptr<IBackupCoordination> backup_coordination;
    ClusterPtr cluster;
    BackupMutablePtr backup;
    std::shared_ptr<ProcessListEntry> process_list_element_holder;

    BackupStarter(BackupsWorker & backups_worker_, const ASTPtr & query_, const ContextPtr & context_)
        : backups_worker(backups_worker_)
        , backup_query(std::static_pointer_cast<ASTBackupQuery>(query_->clone()))
        , query_context(context_)
        , backup_context(Context::createCopy(query_context))
    {
        backup_context->makeQueryContext();
        backup_settings = BackupSettings::fromBackupQuery(*backup_query);
        backup_info = BackupInfo::fromAST(*backup_query->backup_name);
        backup_name_for_logging = backup_info.toStringForLogging();
        on_cluster = !backup_query->cluster.empty();
        is_internal_backup = backup_settings.internal;

        if (!backup_settings.backup_uuid)
            backup_settings.backup_uuid = UUIDHelpers::generateV4();

        /// `backup_id` will be used as a key to the `infos` map, so it should be unique.
        if (is_internal_backup)
            backup_id = "internal-" + toString(UUIDHelpers::generateV4()); /// Always generate `backup_id` for internal backup to avoid collision if both internal and non-internal backups are on the same host
        else if (!backup_settings.id.empty())
            backup_id = backup_settings.id;
        else
            backup_id = toString(*backup_settings.backup_uuid);

        String base_backup_name;
        if (backup_settings.base_backup_info)
            base_backup_name = backup_settings.base_backup_info->toStringForLogging();

        /// process_list_element_holder is used to make an element in ProcessList live while BACKUP is working asynchronously.
        auto process_list_element = backup_context->getProcessListElement();
        if (process_list_element)
            process_list_element_holder = process_list_element->getProcessListEntry();

        backups_worker.addInfo(backup_id,
            backup_name_for_logging,
            base_backup_name,
            backup_context->getCurrentQueryId(),
            is_internal_backup,
            process_list_element,
            BackupStatus::CREATING_BACKUP);
    }

    void makeBackupCoordination()
    {
        chassert(!backup_coordination);
        if (on_cluster)
        {
            backup_query->cluster = backup_context->getMacros()->expand(backup_query->cluster);
            cluster = backup_context->getCluster(backup_query->cluster);
            backup_settings.cluster_host_ids = cluster->getHostIDs();
        }
        bool use_remote_coordination = on_cluster || is_internal_backup;
        backup_coordination = backups_worker.makeBackupCoordination(use_remote_coordination, backup_settings, backup_context);
    }

    void openBackupForWriting()
    {
        chassert(!backup);
        backup = backups_worker.openBackupForWriting(backup_info, backup_settings, backup_coordination, backup_context);
    }

    void doBackup()
    {
        backups_worker.doBackup(
            backup, backup_query, backup_id, backup_name_for_logging, backup_settings, backup_coordination, cluster, backup_context);
    }

    void onException()
    {
        /// Something bad happened, the backup has not built.
        tryLogCurrentException(backups_worker.log, fmt::format("Failed to make {} {}",
                               (is_internal_backup ? "internal backup" : "backup"),
                               backup_name_for_logging));

        bool should_remove_files_in_backup = backup && !is_internal_backup && backups_worker.remove_backup_files_after_failure;

        if (backup && !backup->setIsCorrupted())
            should_remove_files_in_backup = false;

        if (backup_coordination)
        {
            sendCurrentExceptionToCoordination(backup_coordination);
            if (!backup_coordination->tryFinish())
                should_remove_files_in_backup = false;
        }

        if (should_remove_files_in_backup)
            backup->tryRemoveAllFiles();

        if (backup_coordination)
            backup_coordination->tryCleanup();

        backups_worker.setStatusSafe(backup_id, getBackupStatusFromCurrentException());
    }
};


std::pair<BackupOperationID, BackupStatus> BackupsWorker::startMakingBackup(const ASTPtr & query, const ContextPtr & context)
{
    auto starter = std::make_shared<BackupStarter>(*this, query, context);

    try
    {
        if (starter->is_internal_backup)
        {
            /// For internal backups in order to send errors to other hosts via coordination it's better to do it here and not in a separate thread.
            starter->makeBackupCoordination();
            starter->openBackupForWriting();
        }

        auto thread_pool_id = starter->on_cluster ? ThreadPoolId::BACKUP_ASYNC_ON_CLUSTER : ThreadPoolId::BACKUP_ASYNC;
        String thread_name = "BackupWorker";
        auto schedule = threadPoolCallbackRunnerUnsafe<void>(thread_pools->getThreadPool(thread_pool_id), thread_name);

        schedule([starter]
            {
                try
                {
                    if (!starter->is_internal_backup) /// If it's an internal backup then the following is already done.
                    {
                        starter->makeBackupCoordination();
                        starter->openBackupForWriting();
                    }
                    starter->doBackup();
                }
                catch (...)
                {
                    starter->onException();
                }
            },
            Priority{});

        return {starter->backup_id, BackupStatus::CREATING_BACKUP};
    }
    catch (...)
    {
        starter->onException();
        throw;
    }
}


BackupMutablePtr BackupsWorker::openBackupForWriting(const BackupInfo & backup_info, const BackupSettings & backup_settings, std::shared_ptr<IBackupCoordination> backup_coordination, const ContextPtr & context) const
{
    LOG_TRACE(log, "Opening backup for writing");
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
    auto backup = BackupFactory::instance().createBackup(backup_create_params);
    LOG_INFO(log, "Opened backup for writing");
    return backup;
}


void BackupsWorker::doBackup(
    BackupMutablePtr backup,
    const std::shared_ptr<ASTBackupQuery> & backup_query,
    const OperationID & backup_id,
    const String & backup_name_for_logging,
    const BackupSettings & backup_settings,
    std::shared_ptr<IBackupCoordination> backup_coordination,
    const ClusterPtr & cluster,
    ContextMutablePtr context)
{
    bool on_cluster = (cluster != nullptr);

    /// Checks access rights if this is not ON CLUSTER query.
    /// (If this is ON CLUSTER query executeDDLQueryOnCluster() will check access rights later.)
    auto required_access = BackupUtils::getRequiredAccessToBackup(backup_query->elements);
    if (!on_cluster)
        context->checkAccess(required_access);

    maybeSleepForTesting();

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
        Int64 distributed_ddl_task_timeout = backup_coordination->getOnClusterInitializationTimeout().count();
        if (!distributed_ddl_task_timeout)
            distributed_ddl_task_timeout = -1;
        context->setSetting("distributed_ddl_task_timeout", Field{distributed_ddl_task_timeout});
        context->setSetting("distributed_ddl_output_mode", Field{"none"});
        BlockIO io = executeDDLQueryOnCluster(backup_query, context, params);
        CompletedPipelineExecutor{io.pipeline}.execute();

        maybeSleepForTesting();

        /// Wait until all the hosts have written their backup entries.
        backup_coordination->waitForStage(Stage::COMPLETED);
        backup_coordination->setStage(Stage::COMPLETED,"");
    }
    else
    {
        backup_query->setCurrentDatabase(context->getCurrentDatabase());

        auto read_settings = getReadSettingsForBackup(context, backup_settings);

        /// Prepare backup entries.
        BackupEntries backup_entries;
        {
            BackupEntriesCollector backup_entries_collector(
                backup_query->elements, backup_settings, backup_coordination,
                read_settings, context, getThreadPool(ThreadPoolId::BACKUP_MAKE_FILES_LIST));
            backup_entries = backup_entries_collector.run();
        }

        /// Write the backup entries to the backup.
        chassert(backup);
        chassert(backup_coordination);
        chassert(context);
        buildFileInfosForBackupEntries(backup, backup_entries, read_settings, backup_coordination, context->getProcessListElement());
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
    backup->finalizeWriting();
    if (!backup_settings.internal)
    {
        num_files = backup->getNumFiles();
        total_size = backup->getTotalSize();
        num_entries = backup->getNumEntries();
        uncompressed_size = backup->getUncompressedSize();
        compressed_size = backup->getCompressedSize();
    }

    /// Close the backup.
    backup.reset();

    /// The backup coordination is not needed anymore.
    backup_coordination->cleanup();

    /// NOTE: we need to update metadata again after backup->finalizeWriting(), because backup metadata is written there.
    setNumFilesAndSize(backup_id, num_files, total_size, num_entries, uncompressed_size, compressed_size, 0, 0);

    /// NOTE: setStatus is called after setNumFilesAndSize in order to have actual information in a backup log record
    LOG_INFO(log, "{} {} was created successfully", (backup_settings.internal ? "Internal backup" : "Backup"), backup_name_for_logging);
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


struct BackupsWorker::RestoreStarter
{
    BackupsWorker & backups_worker;
    std::shared_ptr<ASTBackupQuery> restore_query;
    ContextPtr query_context; /// We have to keep `query_context` until the end of the operation because a pointer to it is stored inside the ThreadGroup we're using.
    ContextMutablePtr restore_context;
    RestoreSettings restore_settings;
    BackupInfo backup_info;
    String restore_id;
    String backup_name_for_logging;
    bool on_cluster;
    bool is_internal_restore;
    std::shared_ptr<IRestoreCoordination> restore_coordination;
    ClusterPtr cluster;
    std::shared_ptr<ProcessListEntry> process_list_element_holder;

    RestoreStarter(BackupsWorker & backups_worker_, const ASTPtr & query_, const ContextPtr & context_)
        : backups_worker(backups_worker_)
        , restore_query(std::static_pointer_cast<ASTBackupQuery>(query_->clone()))
        , query_context(context_)
        , restore_context(Context::createCopy(query_context))
    {
        restore_context->makeQueryContext();
        restore_settings = RestoreSettings::fromRestoreQuery(*restore_query);
        backup_info = BackupInfo::fromAST(*restore_query->backup_name);
        backup_name_for_logging = backup_info.toStringForLogging();
        on_cluster = !restore_query->cluster.empty();
        is_internal_restore = restore_settings.internal;

        if (!restore_settings.restore_uuid)
            restore_settings.restore_uuid = UUIDHelpers::generateV4();

        /// `restore_id` will be used as a key to the `infos` map, so it should be unique.
        if (is_internal_restore)
            restore_id = "internal-" + toString(UUIDHelpers::generateV4()); /// Always generate `restore_id` for internal restore to avoid collision if both internal and non-internal restores are on the same host
        else if (!restore_settings.id.empty())
            restore_id = restore_settings.id;
        else
            restore_id = toString(*restore_settings.restore_uuid);

        String base_backup_name;
        if (restore_settings.base_backup_info)
            base_backup_name = restore_settings.base_backup_info->toStringForLogging();

        /// process_list_element_holder is used to make an element in ProcessList live while BACKUP is working asynchronously.
        auto process_list_element = restore_context->getProcessListElement();
        if (process_list_element)
            process_list_element_holder = process_list_element->getProcessListEntry();

        backups_worker.addInfo(restore_id,
            backup_name_for_logging,
            base_backup_name,
            restore_context->getCurrentQueryId(),
            is_internal_restore,
            process_list_element,
            BackupStatus::RESTORING);
    }

    void makeRestoreCoordination()
    {
        chassert(!restore_coordination);
        if (on_cluster)
        {
            restore_query->cluster = restore_context->getMacros()->expand(restore_query->cluster);
            cluster = restore_context->getCluster(restore_query->cluster);
            restore_settings.cluster_host_ids = cluster->getHostIDs();
        }
        bool use_remote_coordination = on_cluster || is_internal_restore;
        restore_coordination = backups_worker.makeRestoreCoordination(use_remote_coordination, restore_settings, restore_context);
    }

    void doRestore()
    {
        backups_worker.doRestore(
            restore_query,
            restore_id,
            backup_name_for_logging,
            backup_info,
            restore_settings,
            restore_coordination,
            cluster,
            restore_context);
    }

    void onException()
    {
        /// Something bad happened, some data were not restored.
        tryLogCurrentException(backups_worker.log, fmt::format("Failed to restore from {} {}", (is_internal_restore ? "internal backup" : "backup"), backup_name_for_logging));

        if (restore_coordination)
        {
            sendCurrentExceptionToCoordination(restore_coordination);
            restore_coordination->tryCleanup();
        }

        backups_worker.setStatusSafe(restore_id, getRestoreStatusFromCurrentException());
    }
};


std::pair<BackupOperationID, BackupStatus> BackupsWorker::startRestoring(const ASTPtr & query, ContextMutablePtr context)
{
    auto starter = std::make_shared<RestoreStarter>(*this, query, context);

    try
    {
        if (starter->is_internal_restore)
        {
            /// For internal restores in order to send errors to other hosts via coordination it's better to do it here and not in a separate thread.
            starter->makeRestoreCoordination();
        }

        auto thread_pool_id = starter->on_cluster ? ThreadPoolId::RESTORE_ASYNC_ON_CLUSTER : ThreadPoolId::RESTORE_ASYNC;
        String thread_name = "RestorerWorker";
        auto schedule = threadPoolCallbackRunnerUnsafe<void>(thread_pools->getThreadPool(thread_pool_id), thread_name);

        schedule([starter]
            {
                try
                {
                    if (!starter->is_internal_restore) /// If it's an internal restore then the following is already done.
                    {
                        starter->makeRestoreCoordination();
                    }
                    starter->doRestore();
                }
                catch (...)
                {
                    starter->onException();
                }
            },
            Priority{});

        return {starter->restore_id, BackupStatus::RESTORING};
    }
    catch (...)
    {
        starter->onException();
        throw;
    }
}


BackupPtr BackupsWorker::openBackupForReading(const BackupInfo & backup_info, const RestoreSettings & restore_settings, const ContextPtr & context) const
{
    LOG_TRACE(log, "Opening backup for reading");
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
    auto backup = BackupFactory::instance().createBackup(backup_open_params);
    LOG_TRACE(log, "Opened backup for reading");
    return backup;
}


void BackupsWorker::doRestore(
    const std::shared_ptr<ASTBackupQuery> & restore_query,
    const OperationID & restore_id,
    const String & backup_name_for_logging,
    const BackupInfo & backup_info,
    RestoreSettings restore_settings,
    std::shared_ptr<IRestoreCoordination> restore_coordination,
    const ClusterPtr & cluster,
    ContextMutablePtr context)
{
    maybeSleepForTesting();

    /// Open the backup for reading.
    BackupPtr backup = openBackupForReading(backup_info, restore_settings, context);

    bool on_cluster = (cluster != nullptr);
    String current_database = context->getCurrentDatabase();

    /// Checks access rights if this is ON CLUSTER query.
    /// (If this isn't ON CLUSTER query RestorerFromBackup will check access rights later.)
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
        /// Send the RESTORE query to other hosts.
        DDLQueryOnClusterParams params;
        params.cluster = cluster;
        params.only_shard_num = restore_settings.shard_num;
        params.only_replica_num = restore_settings.replica_num;
        restore_settings.copySettingsToQuery(*restore_query);

        Int64 distributed_ddl_task_timeout = restore_coordination->getOnClusterInitializationTimeout().count();
        if (!distributed_ddl_task_timeout)
            distributed_ddl_task_timeout = -1;
        context->setSetting("distributed_ddl_task_timeout", Field{distributed_ddl_task_timeout});
        context->setSetting("distributed_ddl_output_mode", Field{"none"});
        BlockIO io = executeDDLQueryOnCluster(restore_query, context, params);
        CompletedPipelineExecutor{io.pipeline}.execute();

        maybeSleepForTesting();

        /// Wait until all the hosts have written their backup entries.
        restore_coordination->waitForStage(Stage::COMPLETED);
        restore_coordination->setStage(Stage::COMPLETED,"");
    }
    else
    {
        maybeSleepForTesting();

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

    /// The restore coordination is not needed anymore.
    restore_coordination->cleanup();

    LOG_INFO(log, "Restored from {} {} successfully", (restore_settings.internal ? "internal backup" : "backup"), backup_name_for_logging);
    setStatus(restore_id, BackupStatus::RESTORED);
}


std::shared_ptr<IBackupCoordination>
BackupsWorker::makeBackupCoordination(bool remote, const BackupSettings & backup_settings, const ContextPtr & context) const
{
    if (remote)
    {
        String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
        auto get_zookeeper = [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); };
        auto keeper_settings = BackupKeeperSettings::fromContext(context);

        auto all_hosts = BackupSettings::Util::filterHostIDs(
            backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num);

        String current_host = backup_settings.internal ? backup_settings.host_id : String{BackupCoordinationRemote::kInitiator};

        auto thread_pool_id = backup_settings.internal ? ThreadPoolId::BACKUP_COORDINATION_WORKER : ThreadPoolId::BACKUP_COORDINATION_WORKER_ON_CLUSTER;
        String thread_name = backup_settings.internal ? "BackupCoordWrk" : "BackupCoordInt";
        auto schedule = threadPoolCallbackRunnerUnsafe<void>(thread_pools->getThreadPool(thread_pool_id), thread_name);

        return std::make_shared<BackupCoordinationRemote>(
            *backup_settings.backup_uuid,
            !backup_settings.deduplicate_files,
            root_zk_path,
            get_zookeeper,
            keeper_settings,
            current_host,
            all_hosts,
            *concurrency_checker,
            allow_concurrent_backups,
            schedule,
            context->getProcessListElement());
    }
    else
    {
        return std::make_shared<BackupCoordinationLocal>(!backup_settings.deduplicate_files, *concurrency_checker, allow_concurrent_backups);
    }
}

std::shared_ptr<IRestoreCoordination>
BackupsWorker::makeRestoreCoordination(bool remote, const RestoreSettings & restore_settings, const ContextPtr & context) const
{
    if (remote)
    {
        String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
        auto get_zookeeper = [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); };
        auto keeper_settings = BackupKeeperSettings::fromContext(context);

        auto all_hosts = BackupSettings::Util::filterHostIDs(
            restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num);

        String current_host = restore_settings.internal ? restore_settings.host_id : String{RestoreCoordinationRemote::kInitiator};

        auto thread_pool_id = restore_settings.internal ? ThreadPoolId::RESTORE_COORDINATION_WORKER : ThreadPoolId::RESTORE_COORDINATION_WORKER_ON_CLUSTER;
        String thread_name = restore_settings.internal ? "RestoreCoordWrk" : "RestoreCoordInt";
        auto schedule = threadPoolCallbackRunnerUnsafe<void>(thread_pools->getThreadPool(thread_pool_id), thread_name);

        return std::make_shared<RestoreCoordinationRemote>(
            *restore_settings.restore_uuid,
            root_zk_path,
            get_zookeeper,
            keeper_settings,
            current_host,
            all_hosts,
            *concurrency_checker,
            allow_concurrent_restores,
            schedule,
            context->getProcessListElement());
    }
    else
    {
        return std::make_shared<RestoreCoordinationLocal>(*concurrency_checker, allow_concurrent_restores);
    }
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
        else
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


BackupStatus BackupsWorker::wait(const OperationID & backup_or_restore_id, bool rethrow_exception)
{
    std::unique_lock lock{infos_mutex};
    BackupStatus current_status;
    status_changed.wait(lock, [&]
    {
        auto it = infos.find(backup_or_restore_id);
        if (it == infos.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", backup_or_restore_id);
        const auto & info = it->second.info;
        current_status = info.status;
        if (rethrow_exception && isFailedOrCancelled(current_status))
            std::rethrow_exception(info.exception);
        if (isFinalStatus(current_status))
            return true;
        LOG_INFO(log, "Waiting {} {} to complete", isBackupStatus(current_status) ? "backup" : "restore", info.name);
        return false;
    });
    return current_status;
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

BackupStatus BackupsWorker::cancel(const BackupOperationID & backup_or_restore_id, bool wait_)
{
    QueryStatusPtr process_list_element;
    BackupStatus current_status;

    {
        std::unique_lock lock{infos_mutex};
        auto it = infos.find(backup_or_restore_id);
        if (it == infos.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup ID {}", backup_or_restore_id);

        const auto & extended_info = it->second;
        const auto & info = extended_info.info;
        current_status = info.status;
        if (isFinalStatus(current_status) || !extended_info.process_list_element)
            return current_status;

        LOG_INFO(log, "Cancelling {} {}", isBackupStatus(current_status) ? "backup" : "restore", info.name);
        process_list_element = extended_info.process_list_element;
    }

    process_list.sendCancelToQuery(process_list_element);

    if (!wait_)
        return current_status;

    return wait(backup_or_restore_id, /* rethrow_exception= */ false);
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
