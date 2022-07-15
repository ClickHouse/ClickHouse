#include <Backups/BackupsWorker.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupEntriesCollector.h>
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
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Coordination status meaning that a host finished its work.
    constexpr const char * kCompletedCoordinationStatus = "completed";

    /// Sends information about the current exception to IBackupCoordination or IRestoreCoordination.
    template <typename CoordinationType>
    void sendErrorToCoordination(std::shared_ptr<CoordinationType> coordination, const String & current_host)
    {
        if (!coordination)
            return;
        try
        {
            coordination->setErrorStatus(current_host, Exception{getCurrentExceptionCode(), getCurrentExceptionMessage(true, true)});
        }
        catch (...)
        {
        }
    }
}


BackupsWorker::BackupsWorker(size_t num_backup_threads, size_t num_restore_threads)
    : backups_thread_pool(num_backup_threads, /* max_free_threads = */ 0, num_backup_threads)
    , restores_thread_pool(num_restore_threads, /* max_free_threads = */ 0, num_restore_threads)
    , log(&Poco::Logger::get("BackupsWorker"))
{
    /// We set max_free_threads = 0 because we don't want to keep any threads if there is no BACKUP or RESTORE query running right now.
}

UUID BackupsWorker::start(const ASTPtr & backup_or_restore_query, ContextMutablePtr context)
{
    const ASTBackupQuery & backup_query = typeid_cast<const ASTBackupQuery &>(*backup_or_restore_query);
    if (backup_query.kind == ASTBackupQuery::Kind::BACKUP)
        return startMakingBackup(backup_or_restore_query, context);
    else
        return startRestoring(backup_or_restore_query, context);
}


UUID BackupsWorker::startMakingBackup(const ASTPtr & query, const ContextPtr & context)
{
    auto backup_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto backup_settings = BackupSettings::fromBackupQuery(*backup_query);
    auto backup_info = BackupInfo::fromAST(*backup_query->backup_name);
    bool on_cluster = !backup_query->cluster.empty();

    if (!backup_settings.backup_uuid)
        backup_settings.backup_uuid = UUIDHelpers::generateV4();
    UUID backup_uuid = *backup_settings.backup_uuid;

    /// Prepare context to use.
    ContextPtr context_in_use = context;
    ContextMutablePtr mutable_context;
    if (on_cluster || backup_settings.async)
    {
        /// For ON CLUSTER queries we will need to change some settings.
        /// For ASYNC queries we have to clone the context anyway.
        context_in_use = mutable_context = Context::createCopy(context);
    }

    addInfo(backup_uuid, backup_info.toString(), BackupStatus::MAKING_BACKUP, backup_settings.internal);

    auto job = [this,
                backup_uuid,
                backup_query,
                backup_settings,
                backup_info,
                on_cluster,
                context_in_use,
                mutable_context](bool async) mutable
    {
        std::optional<CurrentThread::QueryScope> query_scope;
        std::shared_ptr<IBackupCoordination> backup_coordination;
        SCOPE_EXIT_SAFE(if (backup_coordination && !backup_settings.internal) backup_coordination->drop(););

        try
        {
            if (async)
            {
                query_scope.emplace(mutable_context);
                setThreadName("BackupWorker");
            }

            /// Checks access rights if this is not ON CLUSTER query.
            /// (If this is ON CLUSTER query executeDDLQueryOnCluster() will check access rights later.)
            auto required_access = getRequiredAccessToBackup(backup_query->elements);
            if (!on_cluster)
                context_in_use->checkAccess(required_access);

            ClusterPtr cluster;
            if (on_cluster)
            {
                backup_query->cluster = context_in_use->getMacros()->expand(backup_query->cluster);
                cluster = context_in_use->getCluster(backup_query->cluster);
                backup_settings.cluster_host_ids = cluster->getHostIDs();
                if (backup_settings.coordination_zk_path.empty())
                {
                    String root_zk_path = context_in_use->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                    backup_settings.coordination_zk_path = root_zk_path + "/backup-" + toString(backup_uuid);
                }
            }

            /// Make a backup coordination.
            if (!backup_settings.coordination_zk_path.empty())
            {
                backup_coordination = std::make_shared<BackupCoordinationRemote>(
                    backup_settings.coordination_zk_path,
                    [global_context = context_in_use->getGlobalContext()] { return global_context->getZooKeeper(); });
            }
            else
            {
                backup_coordination = std::make_shared<BackupCoordinationLocal>();
            }

            /// Opens a backup for writing.
            BackupFactory::CreateParams backup_create_params;
            backup_create_params.open_mode = IBackup::OpenMode::WRITE;
            backup_create_params.context = context_in_use;
            backup_create_params.backup_info = backup_info;
            backup_create_params.base_backup_info = backup_settings.base_backup_info;
            backup_create_params.compression_method = backup_settings.compression_method;
            backup_create_params.compression_level = backup_settings.compression_level;
            backup_create_params.password = backup_settings.password;
            backup_create_params.is_internal_backup = backup_settings.internal;
            backup_create_params.backup_coordination = backup_coordination;
            backup_create_params.backup_uuid = backup_uuid;
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
                backup_coordination->waitStatus(all_hosts, kCompletedCoordinationStatus);
            }
            else
            {
                backup_query->setCurrentDatabase(context_in_use->getCurrentDatabase());

                /// Prepare backup entries.
                BackupEntries backup_entries;
                {
                    BackupEntriesCollector backup_entries_collector{backup_query->elements, backup_settings, backup_coordination, context_in_use};
                    backup_entries = backup_entries_collector.run();
                }

                /// Write the backup entries to the backup.
                writeBackupEntries(backup, std::move(backup_entries), backups_thread_pool);

                /// We have written our backup entries, we need to tell other hosts (they could be waiting for it).
                backup_coordination->setStatus(backup_settings.host_id, kCompletedCoordinationStatus, "");
            }

            /// Finalize backup (write its metadata).
            if (!backup_settings.internal)
                backup->finalizeWriting();

            /// Close the backup.
            backup.reset();

            setStatus(backup_uuid, BackupStatus::BACKUP_COMPLETE);
        }
        catch (...)
        {
            /// Something bad happened, the backup has not built.
            setStatus(backup_uuid, BackupStatus::FAILED_TO_BACKUP);
            sendErrorToCoordination(backup_coordination, backup_settings.host_id);
            if (!async)
                throw;
        }
    };

    if (backup_settings.async)
        backups_thread_pool.scheduleOrThrowOnError([job]() mutable { job(true); });
    else
        job(false);

    return backup_uuid;
}


UUID BackupsWorker::startRestoring(const ASTPtr & query, ContextMutablePtr context)
{
    UUID restore_uuid = UUIDHelpers::generateV4();
    auto restore_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto restore_settings = RestoreSettings::fromRestoreQuery(*restore_query);
    auto backup_info = BackupInfo::fromAST(*restore_query->backup_name);
    bool on_cluster = !restore_query->cluster.empty();

    /// Prepare context to use.
    ContextMutablePtr context_in_use = context;
    if (restore_settings.async || on_cluster)
    {
        /// For ON CLUSTER queries we will need to change some settings.
        /// For ASYNC queries we have to clone the context anyway.
        context_in_use = Context::createCopy(context);
    }

    addInfo(restore_uuid, backup_info.toString(), BackupStatus::RESTORING, restore_settings.internal);

    auto job = [this,
                restore_uuid,
                restore_query,
                restore_settings,
                backup_info,
                on_cluster,
                context_in_use](bool async) mutable
    {
        std::optional<CurrentThread::QueryScope> query_scope;
        std::shared_ptr<IRestoreCoordination> restore_coordination;
        SCOPE_EXIT_SAFE(if (restore_coordination && !restore_settings.internal) restore_coordination->drop(););

        try
        {
            if (async)
            {
                query_scope.emplace(context_in_use);
                setThreadName("RestoreWorker");
            }

            /// Open the backup for reading.
            BackupFactory::CreateParams backup_open_params;
            backup_open_params.open_mode = IBackup::OpenMode::READ;
            backup_open_params.context = context_in_use;
            backup_open_params.backup_info = backup_info;
            backup_open_params.base_backup_info = restore_settings.base_backup_info;
            backup_open_params.password = restore_settings.password;
            BackupPtr backup = BackupFactory::instance().createBackup(backup_open_params);

            String current_database = context_in_use->getCurrentDatabase();

            /// Checks access rights if this is ON CLUSTER query.
            /// (If this isn't ON CLUSTER query RestorerFromBackup will check access rights later.)
            ClusterPtr cluster;
            if (on_cluster)
            {
                restore_query->cluster = context_in_use->getMacros()->expand(restore_query->cluster);
                cluster = context_in_use->getCluster(restore_query->cluster);
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
                    RestorerFromBackup dummy_restorer{restore_elements, restore_settings, nullptr, backup, context_in_use};
                    dummy_restorer.run(RestorerFromBackup::CHECK_ACCESS_ONLY);
                }
            }

            /// Make a restore coordination.
            if (on_cluster && restore_settings.coordination_zk_path.empty())
            {
                String root_zk_path = context_in_use->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                restore_settings.coordination_zk_path = root_zk_path + "/restore-" + toString(restore_uuid);
            }

            if (!restore_settings.coordination_zk_path.empty())
            {
                restore_coordination = std::make_shared<RestoreCoordinationRemote>(
                    restore_settings.coordination_zk_path,
                    [global_context = context_in_use->getGlobalContext()] { return global_context->getZooKeeper(); });
            }
            else
            {
                restore_coordination = std::make_shared<RestoreCoordinationLocal>();
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
                context_in_use->setSetting("distributed_ddl_task_timeout", Field{0});
                context_in_use->setSetting("distributed_ddl_output_mode", Field{"none"});

                executeDDLQueryOnCluster(restore_query, context_in_use, params);

                /// Wait until all the hosts have written their backup entries.
                auto all_hosts = BackupSettings::Util::filterHostIDs(
                    restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num);
                restore_coordination->waitStatus(all_hosts, kCompletedCoordinationStatus);
            }
            else
            {
                restore_query->setCurrentDatabase(current_database);

                /// Restore metadata and prepare data restoring tasks.
                DataRestoreTasks data_restore_tasks;
                {
                    RestorerFromBackup restorer{restore_query->elements, restore_settings, restore_coordination,
                                                backup, context_in_use};
                    data_restore_tasks = restorer.run(RestorerFromBackup::RESTORE);
                }

                /// Execute the data restoring tasks.
                restoreTablesData(std::move(data_restore_tasks), restores_thread_pool);

                /// We have restored everything, we need to tell other hosts (they could be waiting for it).
                restore_coordination->setStatus(restore_settings.host_id, kCompletedCoordinationStatus, "");
            }

            setStatus(restore_uuid, BackupStatus::RESTORED);
        }
        catch (...)
        {
            /// Something bad happened, the backup has not built.
            setStatus(restore_uuid, BackupStatus::FAILED_TO_RESTORE);
            sendErrorToCoordination(restore_coordination, restore_settings.host_id);
            if (!async)
                throw;
        }
    };

    if (restore_settings.async)
        backups_thread_pool.scheduleOrThrowOnError([job]() mutable { job(true); });
    else
        job(false);

    return restore_uuid;
}


void BackupsWorker::addInfo(const UUID & uuid, const String & backup_name, BackupStatus status, bool internal)
{
    Info info;
    info.uuid = uuid;
    info.backup_name = backup_name;
    info.status = status;
    info.status_changed_time = time(nullptr);
    info.internal = internal;
    std::lock_guard lock{infos_mutex};
    infos[uuid] = std::move(info);
}

void BackupsWorker::setStatus(const UUID & uuid, BackupStatus status)
{
    std::lock_guard lock{infos_mutex};
    auto & info = infos.at(uuid);
    info.status = status;
    info.status_changed_time = time(nullptr);

    if (status == BackupStatus::BACKUP_COMPLETE)
    {
        LOG_INFO(log, "{} {} was created successfully", (info.internal ? "Internal backup" : "Backup"), info.backup_name);
    }
    else if (status == BackupStatus::RESTORED)
    {
        LOG_INFO(log, "Restored from {} {} successfully", (info.internal ? "internal backup" : "backup"), info.backup_name);
    }
    else if ((status == BackupStatus::FAILED_TO_BACKUP) || (status == BackupStatus::FAILED_TO_RESTORE))
    {
        String start_of_message;
        if (status == BackupStatus::FAILED_TO_BACKUP)
            start_of_message = fmt::format("Failed to create {} {}", (info.internal ? "internal backup" : "backup"), info.backup_name);
        else
            start_of_message = fmt::format("Failed to restore from {} {}", (info.internal ? "internal backup" : "backup"), info.backup_name);
        tryLogCurrentException(log, start_of_message);

        info.error_message = getCurrentExceptionMessage(false);
        info.exception = std::current_exception();
    }
}


void BackupsWorker::wait(const UUID & backup_or_restore_uuid, bool rethrow_exception)
{
    std::unique_lock lock{infos_mutex};
    status_changed.wait(lock, [&]
    {
        auto it = infos.find(backup_or_restore_uuid);
        if (it == infos.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BackupsWorker: Unknown UUID {}", toString(backup_or_restore_uuid));
        const auto & info = it->second;
        auto current_status = info.status;
        if (rethrow_exception && ((current_status == BackupStatus::FAILED_TO_BACKUP) || (current_status == BackupStatus::FAILED_TO_RESTORE)))
            std::rethrow_exception(info.exception);
        return (current_status == BackupStatus::BACKUP_COMPLETE) || (current_status == BackupStatus::RESTORED);
    });
}

BackupsWorker::Info BackupsWorker::getInfo(const UUID & backup_or_restore_uuid) const
{
    std::lock_guard lock{infos_mutex};
    auto it = infos.find(backup_or_restore_uuid);
    if (it == infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BackupsWorker: Unknown UUID {}", toString(backup_or_restore_uuid));
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
    size_t num_active_backups = backups_thread_pool.active();
    size_t num_active_restores = restores_thread_pool.active();
    if (!num_active_backups && !num_active_restores)
        return;
    LOG_INFO(log, "Waiting for {} backup and {} restore tasks to be finished", num_active_backups, num_active_restores);
    backups_thread_pool.wait();
    restores_thread_pool.wait();
    LOG_INFO(log, "All backup and restore tasks have finished");
}

}
