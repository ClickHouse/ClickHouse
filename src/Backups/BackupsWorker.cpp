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
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTBackupQuery.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

using OperationID = BackupsWorker::OperationID;
namespace Stage = BackupCoordinationStage;

namespace
{
    std::shared_ptr<IBackupCoordination> makeBackupCoordination(const String & coordination_zk_path, const ContextPtr & context, bool is_internal_backup)
    {
        if (!coordination_zk_path.empty())
        {
            auto get_zookeeper = [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); };
            return std::make_shared<BackupCoordinationRemote>(coordination_zk_path, get_zookeeper, !is_internal_backup);
        }
        else
        {
            return std::make_shared<BackupCoordinationLocal>();
        }
    }

    std::shared_ptr<IRestoreCoordination> makeRestoreCoordination(const String & coordination_zk_path, const ContextPtr & context, bool is_internal_backup)
    {
        if (!coordination_zk_path.empty())
        {
            auto get_zookeeper = [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); };
            return std::make_shared<RestoreCoordinationRemote>(coordination_zk_path, get_zookeeper, !is_internal_backup);
        }
        else
        {
            return std::make_shared<RestoreCoordinationLocal>();
        }
    }

    /// Sends information about an exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendExceptionToCoordination(std::shared_ptr<CoordinationType> coordination, const String & current_host, const Exception & exception)
    {
        try
        {
            if (coordination)
                coordination->setError(current_host, exception);
        }
        catch (...)
        {
        }
    }

    /// Sends information about the current exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendCurrentExceptionToCoordination(std::shared_ptr<CoordinationType> coordination, const String & current_host)
    {
        try
        {
            throw;
        }
        catch (const Exception & e)
        {
            sendExceptionToCoordination(coordination, current_host, e);
        }
        catch (...)
        {
            coordination->setError(current_host, Exception{getCurrentExceptionCode(), getCurrentExceptionMessage(true, true)});
        }
    }

    bool isFinalStatus(BackupStatus status)
    {
        return (status == BackupStatus::BACKUP_CREATED) || (status == BackupStatus::BACKUP_FAILED) || (status == BackupStatus::RESTORED)
            || (status == BackupStatus::RESTORE_FAILED);
    }

    bool isErrorStatus(BackupStatus status)
    {
        return (status == BackupStatus::BACKUP_FAILED) || (status == BackupStatus::RESTORE_FAILED);
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
}


BackupsWorker::BackupsWorker(size_t num_backup_threads, size_t num_restore_threads)
    : backups_thread_pool(num_backup_threads, /* max_free_threads = */ 0, num_backup_threads)
    , restores_thread_pool(num_restore_threads, /* max_free_threads = */ 0, num_restore_threads)
    , log(&Poco::Logger::get("BackupsWorker"))
{
    /// We set max_free_threads = 0 because we don't want to keep any threads if there is no BACKUP or RESTORE query running right now.
}


OperationID BackupsWorker::start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context)
{
    const ASTBackupQuery & backup_query = typeid_cast<const ASTBackupQuery &>(*backup_or_restore_query);
    if (backup_query.kind == ASTBackupQuery::Kind::BACKUP)
        return startMakingBackup(backup_or_restore_query, context);
    else
        return startRestoring(backup_or_restore_query, context);
}


OperationID BackupsWorker::startMakingBackup(const ASTPtr & query, const ContextPtr & context)
{
    auto backup_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto backup_settings = BackupSettings::fromBackupQuery(*backup_query);

    if (!backup_settings.backup_uuid)
        backup_settings.backup_uuid = UUIDHelpers::generateV4();

    OperationID backup_id = backup_settings.id;
    if (backup_id.empty())
        backup_id = toString(*backup_settings.backup_uuid);

    std::shared_ptr<IBackupCoordination> backup_coordination;

    if (backup_settings.internal)
    {
        /// The following call of makeBackupCoordination() is not essential because doBackup() will later create a backup coordination
        /// if it's not created here. However to handle errors better it's better to make a coordination here because this way
        /// if an exception will be thrown in startMakingBackup() other hosts will know about that.
        backup_coordination = makeBackupCoordination(backup_settings.coordination_zk_path, context, backup_settings.internal);
    }

    try
    {
        auto backup_info = BackupInfo::fromAST(*backup_query->backup_name);

        if (!backup_settings.internal)
            addInfo(backup_id, backup_info.toString(), BackupStatus::CREATING_BACKUP);

        /// Prepare context to use.
        ContextPtr context_in_use = context;
        ContextMutablePtr mutable_context;
        bool on_cluster = !backup_query->cluster.empty();
        if (on_cluster || backup_settings.async)
        {
            /// For ON CLUSTER queries we will need to change some settings.
            /// For ASYNC queries we have to clone the context anyway.
            context_in_use = mutable_context = Context::createCopy(context);
        }

        if (backup_settings.async)
        {
            backups_thread_pool.scheduleOrThrowOnError(
                [this, backup_query, backup_id, backup_settings, backup_info, backup_coordination, context_in_use, mutable_context]
                {
                    doBackup(
                        backup_query,
                        backup_id,
                        backup_settings,
                        backup_info,
                        backup_coordination,
                        context_in_use,
                        mutable_context,
                        /* called_async= */ true);
                });
        }
        else
        {
            doBackup(
                backup_query,
                backup_id,
                backup_settings,
                backup_info,
                backup_coordination,
                context_in_use,
                mutable_context,
                /* called_async= */ false);
        }

        return backup_id;
    }
    catch (...)
    {
        /// Something bad happened, the backup has not built.
        if (!backup_settings.internal)
            setStatus(backup_id, BackupStatus::BACKUP_FAILED);
        sendCurrentExceptionToCoordination(backup_coordination, backup_settings.host_id);
        throw;
    }
}


void BackupsWorker::doBackup(
    const std::shared_ptr<ASTBackupQuery> & backup_query,
    const OperationID & backup_id,
    BackupSettings backup_settings,
    const BackupInfo & backup_info,
    std::shared_ptr<IBackupCoordination> backup_coordination,
    const ContextPtr & context,
    ContextMutablePtr mutable_context,
    bool called_async)
{
    std::optional<CurrentThread::QueryScope> query_scope;
    try
    {
        if (called_async)
        {
            query_scope.emplace(mutable_context);
            setThreadName("BackupWorker");
        }

        bool on_cluster = !backup_query->cluster.empty();
        assert(mutable_context || (!on_cluster && !called_async));

        /// Checks access rights if this is not ON CLUSTER query.
        /// (If this is ON CLUSTER query executeDDLQueryOnCluster() will check access rights later.)
        auto required_access = getRequiredAccessToBackup(backup_query->elements);
        if (!on_cluster)
            context->checkAccess(required_access);

        ClusterPtr cluster;
        if (on_cluster)
        {
            backup_query->cluster = context->getMacros()->expand(backup_query->cluster);
            cluster = context->getCluster(backup_query->cluster);
            backup_settings.cluster_host_ids = cluster->getHostIDs();
            if (backup_settings.coordination_zk_path.empty())
            {
                String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                backup_settings.coordination_zk_path = root_zk_path + "/backup-" + toString(*backup_settings.backup_uuid);
            }
        }

        /// Make a backup coordination.
        if (!backup_coordination)
            backup_coordination = makeBackupCoordination(backup_settings.coordination_zk_path, context, backup_settings.internal);

        /// Opens a backup for writing.
        BackupFactory::CreateParams backup_create_params;
        backup_create_params.open_mode = IBackup::OpenMode::WRITE;
        backup_create_params.context = context;
        backup_create_params.backup_info = backup_info;
        backup_create_params.base_backup_info = backup_settings.base_backup_info;
        backup_create_params.compression_method = backup_settings.compression_method;
        backup_create_params.compression_level = backup_settings.compression_level;
        backup_create_params.password = backup_settings.password;
        backup_create_params.is_internal_backup = backup_settings.internal;
        backup_create_params.backup_coordination = backup_coordination;
        backup_create_params.backup_uuid = backup_settings.backup_uuid;
        BackupMutablePtr backup = BackupFactory::instance().createBackup(backup_create_params);

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
            auto all_hosts = BackupSettings::Util::filterHostIDs(
                backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num);
            backup_coordination->waitForStage(all_hosts, Stage::COMPLETED);
        }
        else
        {
            backup_query->setCurrentDatabase(context->getCurrentDatabase());

            /// Prepare backup entries.
            BackupEntries backup_entries;
            {
                BackupEntriesCollector backup_entries_collector{backup_query->elements, backup_settings, backup_coordination, context};
                backup_entries = backup_entries_collector.run();
            }

            /// Write the backup entries to the backup.
            writeBackupEntries(backup, std::move(backup_entries), backups_thread_pool);

            /// We have written our backup entries, we need to tell other hosts (they could be waiting for it).
            backup_coordination->setStage(backup_settings.host_id, Stage::COMPLETED, "");
        }

        size_t num_files = 0;
        UInt64 uncompressed_size = 0;
        UInt64 compressed_size = 0;

        /// Finalize backup (write its metadata).
        if (!backup_settings.internal)
        {
            backup->finalizeWriting();
            num_files = backup->getNumFiles();
            uncompressed_size = backup->getUncompressedSize();
            compressed_size = backup->getCompressedSize();
        }

        /// Close the backup.
        backup.reset();

        LOG_INFO(log, "{} {} was created successfully", (backup_settings.internal ? "Internal backup" : "Backup"), backup_info.toString());
        if (!backup_settings.internal)
        {
            setStatus(backup_id, BackupStatus::BACKUP_CREATED);
            setNumFilesAndSize(backup_id, num_files, uncompressed_size, compressed_size);
        }
    }
    catch (...)
    {
        /// Something bad happened, the backup has not built.
        if (called_async)
        {
            tryLogCurrentException(log, fmt::format("Failed to make {} {}", (backup_settings.internal ? "internal backup" : "backup"), backup_info.toString()));
            if (!backup_settings.internal)
                setStatus(backup_id, BackupStatus::BACKUP_FAILED);
            sendCurrentExceptionToCoordination(backup_coordination, backup_settings.host_id);
        }
        else
        {
            /// setStatus() and sendCurrentExceptionToCoordination() will be called by startMakingBackup().
            throw;
        }
    }
}


OperationID BackupsWorker::startRestoring(const ASTPtr & query, ContextMutablePtr context)
{
    auto restore_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto restore_settings = RestoreSettings::fromRestoreQuery(*restore_query);

    UUID restore_uuid = UUIDHelpers::generateV4();

    OperationID restore_id = restore_settings.id;
    if (restore_id.empty())
        restore_id = toString(restore_uuid);

    std::shared_ptr<IRestoreCoordination> restore_coordination;

    if (restore_settings.internal)
    {
        /// The following call of makeRestoreCoordination() is not essential because doRestore() will later create a restore coordination
        /// if it's not created here. However to handle errors better it's better to make a coordination here because this way
        /// if an exception will be thrown in startRestoring() other hosts will know about that.
        restore_coordination = makeRestoreCoordination(restore_settings.coordination_zk_path, context, restore_settings.internal);
    }

    try
    {
        auto backup_info = BackupInfo::fromAST(*restore_query->backup_name);
        if (!restore_settings.internal)
            addInfo(restore_id, backup_info.toString(), BackupStatus::RESTORING);

        /// Prepare context to use.
        ContextMutablePtr context_in_use = context;
        bool on_cluster = !restore_query->cluster.empty();
        if (restore_settings.async || on_cluster)
        {
            /// For ON CLUSTER queries we will need to change some settings.
            /// For ASYNC queries we have to clone the context anyway.
            context_in_use = Context::createCopy(context);
        }

        if (restore_settings.async)
        {
            backups_thread_pool.scheduleOrThrowOnError(
                [this, restore_query, restore_id, restore_uuid, restore_settings, backup_info, restore_coordination, context_in_use] {
                    doRestore(
                        restore_query,
                        restore_id,
                        restore_uuid,
                        restore_settings,
                        backup_info,
                        restore_coordination,
                        context_in_use,
                        /* called_async= */ true);
                });
        }
        else
        {
            doRestore(
                restore_query,
                restore_id,
                restore_uuid,
                restore_settings,
                backup_info,
                restore_coordination,
                context_in_use,
                /* called_async= */ false);
        }

        return restore_id;
    }
    catch (...)
    {
        /// Something bad happened, the backup has not built.
        if (!restore_settings.internal)
            setStatus(restore_id, BackupStatus::RESTORE_FAILED);
        sendCurrentExceptionToCoordination(restore_coordination, restore_settings.host_id);
        throw;
    }
}


void BackupsWorker::doRestore(
    const std::shared_ptr<ASTBackupQuery> & restore_query,
    const OperationID & restore_id,
    const UUID & restore_uuid,
    RestoreSettings restore_settings,
    const BackupInfo & backup_info,
    std::shared_ptr<IRestoreCoordination> restore_coordination,
    ContextMutablePtr context,
    bool called_async)
{
    std::optional<CurrentThread::QueryScope> query_scope;
    try
    {
        if (called_async)
        {
            query_scope.emplace(context);
            setThreadName("RestoreWorker");
        }

        /// Open the backup for reading.
        BackupFactory::CreateParams backup_open_params;
        backup_open_params.open_mode = IBackup::OpenMode::READ;
        backup_open_params.context = context;
        backup_open_params.backup_info = backup_info;
        backup_open_params.base_backup_info = restore_settings.base_backup_info;
        backup_open_params.password = restore_settings.password;
        BackupPtr backup = BackupFactory::instance().createBackup(backup_open_params);

        if (!restore_settings.internal)
            setNumFilesAndSize(restore_id, backup->getNumFiles(), backup->getUncompressedSize(), backup->getCompressedSize());

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
                RestorerFromBackup dummy_restorer{restore_elements, restore_settings, nullptr, backup, context};
                dummy_restorer.run(RestorerFromBackup::CHECK_ACCESS_ONLY);
            }
        }

        /// Make a restore coordination.
        if (on_cluster && restore_settings.coordination_zk_path.empty())
        {
            String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
            restore_settings.coordination_zk_path = root_zk_path + "/restore-" + toString(restore_uuid);
        }

        if (!restore_coordination)
            restore_coordination = makeRestoreCoordination(restore_settings.coordination_zk_path, context, restore_settings.internal);

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
            auto all_hosts = BackupSettings::Util::filterHostIDs(
                restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num);
            restore_coordination->waitForStage(all_hosts, Stage::COMPLETED);
        }
        else
        {
            restore_query->setCurrentDatabase(current_database);

            /// Restore metadata and prepare data restoring tasks.
            DataRestoreTasks data_restore_tasks;
            {
                RestorerFromBackup restorer{restore_query->elements, restore_settings, restore_coordination,
                                            backup, context};
                data_restore_tasks = restorer.run(RestorerFromBackup::RESTORE);
            }

            /// Execute the data restoring tasks.
            restoreTablesData(std::move(data_restore_tasks), restores_thread_pool);

            /// We have restored everything, we need to tell other hosts (they could be waiting for it).
            restore_coordination->setStage(restore_settings.host_id, Stage::COMPLETED, "");
        }

        LOG_INFO(log, "Restored from {} {} successfully", (restore_settings.internal ? "internal backup" : "backup"), backup_info.toString());
        if (!restore_settings.internal)
            setStatus(restore_id, BackupStatus::RESTORED);
    }
    catch (...)
    {
        /// Something bad happened, the backup has not built.
        if (called_async)
        {
            tryLogCurrentException(log, fmt::format("Failed to restore from {} {}", (restore_settings.internal ? "internal backup" : "backup"), backup_info.toString()));
            if (!restore_settings.internal)
                setStatus(restore_id, BackupStatus::RESTORE_FAILED);
            sendCurrentExceptionToCoordination(restore_coordination, restore_settings.host_id);
        }
        else
        {
            /// setStatus() and sendCurrentExceptionToCoordination() will be called by startRestoring().
            throw;
        }
    }
}


void BackupsWorker::addInfo(const OperationID & id, const String & name, BackupStatus status)
{
    Info info;
    info.id = id;
    info.name = name;
    info.status = status;
    info.start_time = std::chrono::system_clock::now();

    if (isFinalStatus(status))
        info.end_time = info.start_time;

    std::lock_guard lock{infos_mutex};

    auto it = infos.find(id);
    if (it != infos.end())
    {
        /// It's better not allow to overwrite the current status if it's in progress.
        auto current_status = it->second.status;
        if (!isFinalStatus(current_status))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot start a backup or restore: it's id='{}' is already in use", id);
    }

    infos[id] = std::move(info);

    num_active_backups += getNumActiveBackupsChange(status);
    num_active_restores += getNumActiveRestoresChange(status);
}


void BackupsWorker::setStatus(const String & id, BackupStatus status)
{
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(id);
    if (it == infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup's id={}", id);

    auto & info = it->second;
    auto old_status = info.status;

    info.status = status;

    if (isFinalStatus(status))
        info.end_time = std::chrono::system_clock::now();

    if (isErrorStatus(status))
    {
        info.error_message = getCurrentExceptionMessage(false);
        info.exception = std::current_exception();
    }

    num_active_backups += getNumActiveBackupsChange(status) - getNumActiveBackupsChange(old_status);
    num_active_restores += getNumActiveRestoresChange(status) - getNumActiveRestoresChange(old_status);
}


void BackupsWorker::setNumFilesAndSize(const String & id, size_t num_files, UInt64 uncompressed_size, UInt64 compressed_size)
{
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(id);
    if (it == infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup's id={}", id);

    auto & info = it->second;
    info.num_files = num_files;
    info.uncompressed_size = uncompressed_size;
    info.compressed_size = compressed_size;
}


void BackupsWorker::wait(const OperationID & id, bool rethrow_exception)
{
    std::unique_lock lock{infos_mutex};
    status_changed.wait(lock, [&]
    {
        auto it = infos.find(id);
        if (it == infos.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup's id={}", id);
        const auto & info = it->second;
        auto current_status = info.status;
        if (rethrow_exception && isErrorStatus(current_status))
            std::rethrow_exception(info.exception);
        return isFinalStatus(current_status);
    });
}

BackupsWorker::Info BackupsWorker::getInfo(const OperationID & id) const
{
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(id);
    if (it == infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup's id={}", id);
    return it->second;
}

std::vector<BackupsWorker::Info> BackupsWorker::getAllInfos() const
{
    std::vector<Info> res_infos;
    std::lock_guard lock{infos_mutex};
    for (const auto & info : infos | boost::adaptors::map_values)
        res_infos.push_back(info);
    return res_infos;
}

void BackupsWorker::shutdown()
{
    bool has_active_backups_and_restores = (num_active_backups || num_active_restores);
    if (has_active_backups_and_restores)
        LOG_INFO(log, "Waiting for {} backups and {} restores to be finished", num_active_backups, num_active_restores);

    backups_thread_pool.wait();
    restores_thread_pool.wait();

    if (has_active_backups_and_restores)
        LOG_INFO(log, "All backup and restore tasks have finished");
}

}
