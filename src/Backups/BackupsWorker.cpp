#include <Backups/BackupsWorker.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupCoordinationDistributed.h>
#include <Backups/BackupCoordinationLocal.h>
#include <Backups/IRestoreTask.h>
#include <Backups/RestoreCoordinationDistributed.h>
#include <Backups/RestoreCoordinationLocal.h>
#include <Backups/RestoreSettings.h>
#include <Backups/RestoreUtils.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTBackupQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
    UUID backup_uuid = UUIDHelpers::generateV4();
    auto backup_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto backup_info = BackupInfo::fromAST(*backup_query->backup_name);
    auto backup_settings = BackupSettings::fromBackupQuery(*backup_query);

    addInfo(backup_uuid, backup_info.toString(), BackupStatus::MAKING_BACKUP, backup_settings.internal);

    std::shared_ptr<IBackupCoordination> backup_coordination;
    SCOPE_EXIT({
        if (backup_coordination && !backup_settings.internal)
            backup_coordination->drop();
    });

    BackupMutablePtr backup;
    ContextPtr cloned_context;
    bool on_cluster = !backup_query->cluster.empty();
    std::shared_ptr<BlockIO> on_cluster_io;

    try
    {
        auto access_to_check = getRequiredAccessToBackup(backup_query->elements, backup_settings);
        if (!on_cluster)
            context->checkAccess(access_to_check);

        ClusterPtr cluster;
        if (on_cluster)
        {
            backup_query->cluster = context->getMacros()->expand(backup_query->cluster);
            cluster = context->getCluster(backup_query->cluster);
            backup_settings.cluster_host_ids = cluster->getHostIDs();
            if (backup_settings.coordination_zk_path.empty())
            {
                String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                backup_settings.coordination_zk_path = root_zk_path + "/backup-" + toString(backup_uuid);
            }
            backup_settings.copySettingsToQuery(*backup_query);
        }

        if (!backup_settings.coordination_zk_path.empty())
            backup_coordination = std::make_shared<BackupCoordinationDistributed>(
                backup_settings.coordination_zk_path,
                [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); });
        else
            backup_coordination = std::make_shared<BackupCoordinationLocal>();

        BackupFactory::CreateParams backup_create_params;
        backup_create_params.open_mode = IBackup::OpenMode::WRITE;
        backup_create_params.context = context;
        backup_create_params.backup_info = backup_info;
        backup_create_params.base_backup_info = backup_settings.base_backup_info;
        backup_create_params.compression_method = backup_settings.compression_method;
        backup_create_params.compression_level = backup_settings.compression_level;
        backup_create_params.password = backup_settings.password;
        backup_create_params.backup_uuid = backup_uuid;
        backup_create_params.is_internal_backup = backup_settings.internal;
        backup_create_params.backup_coordination = backup_coordination;
        backup = BackupFactory::instance().createBackup(backup_create_params);

        ContextMutablePtr mutable_context;
        if (on_cluster || backup_settings.async)
            cloned_context = mutable_context = Context::createCopy(context);
        else
            cloned_context = context; /// No need to clone context

        if (on_cluster)
        {
            DDLQueryOnClusterParams params;
            params.cluster = cluster;
            params.only_shard_num = backup_settings.shard_num;
            params.only_replica_num = backup_settings.replica_num;
            params.access_to_check = access_to_check;
            mutable_context->setSetting("distributed_ddl_task_timeout", -1); // No timeout
            mutable_context->setSetting("distributed_ddl_output_mode", Field{"throw"});
            auto res = executeDDLQueryOnCluster(backup_query, mutable_context, params);
            on_cluster_io = std::make_shared<BlockIO>(std::move(res));
        }
    }
    catch (...)
    {
        setStatus(backup_uuid, BackupStatus::FAILED_TO_BACKUP);
        throw;
    }

    auto job = [this,
                backup,
                backup_uuid,
                backup_query,
                backup_settings,
                backup_coordination,
                on_cluster_io,
                cloned_context](bool in_separate_thread)
    {
        try
        {
            if (on_cluster_io)
            {
                PullingPipelineExecutor executor(on_cluster_io->pipeline);
                Block block;
                while (executor.pull(block))
                    ;
                backup->finalizeWriting();
            }
            else
            {
                std::optional<CurrentThread::QueryScope> query_scope;
                if (in_separate_thread)
                    query_scope.emplace(cloned_context);

                backup_query->setDatabase(cloned_context->getCurrentDatabase());

                auto timeout_for_preparing = std::chrono::seconds{cloned_context->getConfigRef().getInt("backups.backup_prepare_timeout", -1)};
                auto backup_entries
                    = makeBackupEntries(cloned_context, backup_query->elements, backup_settings, backup_coordination, timeout_for_preparing);
                writeBackupEntries(backup, std::move(backup_entries), backups_thread_pool);
            }
            setStatus(backup_uuid, BackupStatus::BACKUP_COMPLETE);
        }
        catch (...)
        {
            setStatus(backup_uuid, BackupStatus::FAILED_TO_BACKUP);
            if (!in_separate_thread)
                throw;
        }
    };

    if (backup_settings.async)
        backups_thread_pool.scheduleOrThrowOnError([job] { job(true); });
    else
        job(false);

    return backup_uuid;
}


UUID BackupsWorker::startRestoring(const ASTPtr & query, ContextMutablePtr context)
{
    UUID restore_uuid = UUIDHelpers::generateV4();
    auto restore_query = std::static_pointer_cast<ASTBackupQuery>(query->clone());
    auto backup_info = BackupInfo::fromAST(*restore_query->backup_name);
    auto restore_settings = RestoreSettings::fromRestoreQuery(*restore_query);

    addInfo(restore_uuid, backup_info.toString(), BackupStatus::RESTORING, restore_settings.internal);

    std::shared_ptr<IRestoreCoordination> restore_coordination;
    SCOPE_EXIT({
        if (restore_coordination && !restore_settings.internal)
            restore_coordination->drop();
    });

    ContextMutablePtr cloned_context;
    std::shared_ptr<BlockIO> on_cluster_io;
    bool on_cluster = !restore_query->cluster.empty();

    try
    {
        auto access_to_check = getRequiredAccessToRestore(restore_query->elements, restore_settings);
        if (!on_cluster)
            context->checkAccess(access_to_check);

        ClusterPtr cluster;
        if (on_cluster)
        {
            restore_query->cluster = context->getMacros()->expand(restore_query->cluster);
            cluster = context->getCluster(restore_query->cluster);
            restore_settings.cluster_host_ids = cluster->getHostIDs();
            if (restore_settings.coordination_zk_path.empty())
            {
                String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                restore_settings.coordination_zk_path = root_zk_path + "/restore-" + toString(restore_uuid);
            }
            restore_settings.copySettingsToQuery(*restore_query);
        }

        if (!restore_settings.coordination_zk_path.empty())
            restore_coordination = std::make_shared<RestoreCoordinationDistributed>(
                restore_settings.coordination_zk_path,
                [global_context = context->getGlobalContext()] { return global_context->getZooKeeper(); });
        else
            restore_coordination = std::make_shared<RestoreCoordinationLocal>();

        if (on_cluster || restore_settings.async)
            cloned_context = Context::createCopy(context);
        else
            cloned_context = context; /// No need to clone context

        if (on_cluster)
        {
            DDLQueryOnClusterParams params;
            params.cluster = cluster;
            params.only_shard_num = restore_settings.shard_num;
            params.only_replica_num = restore_settings.replica_num;
            params.access_to_check = access_to_check;
            cloned_context->setSetting("distributed_ddl_task_timeout", -1); // No timeout
            cloned_context->setSetting("distributed_ddl_output_mode", Field{"throw"});
            auto res = executeDDLQueryOnCluster(restore_query, cloned_context, params);
            on_cluster_io = std::make_shared<BlockIO>(std::move(res));
        }
    }
    catch (...)
    {
        setStatus(restore_uuid, BackupStatus::FAILED_TO_RESTORE);
        throw;
    }

    auto job = [this,
                backup_info,
                restore_uuid,
                restore_query,
                restore_settings,
                restore_coordination,
                on_cluster_io,
                cloned_context](bool in_separate_thread)
    {
        try
        {
            if (on_cluster_io)
            {
                PullingPipelineExecutor executor(on_cluster_io->pipeline);
                Block block;
                while (executor.pull(block))
                    ;
            }
            else
            {
                std::optional<CurrentThread::QueryScope> query_scope;
                if (in_separate_thread)
                    query_scope.emplace(cloned_context);

                restore_query->setDatabase(cloned_context->getCurrentDatabase());

                BackupFactory::CreateParams backup_open_params;
                backup_open_params.open_mode = IBackup::OpenMode::READ;
                backup_open_params.context = cloned_context;
                backup_open_params.backup_info = backup_info;
                backup_open_params.base_backup_info = restore_settings.base_backup_info;
                backup_open_params.password = restore_settings.password;
                BackupPtr backup = BackupFactory::instance().createBackup(backup_open_params);

                auto timeout_for_restoring_metadata
                    = std::chrono::seconds{cloned_context->getConfigRef().getInt("backups.restore_metadata_timeout", -1)};
                auto restore_tasks = makeRestoreTasks(
                    cloned_context, backup, restore_query->elements, restore_settings, restore_coordination, timeout_for_restoring_metadata);
                restoreMetadata(restore_tasks, restore_settings, restore_coordination, timeout_for_restoring_metadata);
                restoreData(restore_tasks, restores_thread_pool);
            }

            setStatus(restore_uuid, BackupStatus::RESTORED);
        }
        catch (...)
        {
            setStatus(restore_uuid, BackupStatus::FAILED_TO_RESTORE);
            if (!in_separate_thread)
                throw;
        }
    };

    if (restore_settings.async)
        backups_thread_pool.scheduleOrThrowOnError([job] { job(true); });
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

    if ((status == BackupStatus::FAILED_TO_BACKUP) || (status == BackupStatus::FAILED_TO_RESTORE))
    {
        info.error_message = getCurrentExceptionMessage(false);
        info.exception = std::current_exception();
    }

    switch (status)
    {
        case BackupStatus::BACKUP_COMPLETE:
            LOG_INFO(log, "{} {} was created successfully", (info.internal ? "Internal backup" : "Backup"), info.backup_name);
            break;
        case BackupStatus::FAILED_TO_BACKUP:
            LOG_ERROR(log, "Failed to create {} {}", (info.internal ? "internal backup" : "backup"), info.backup_name);
            break;
        case BackupStatus::RESTORED:
            LOG_INFO(log, "Restored from {} {} successfully", (info.internal ? "internal backup" : "backup"), info.backup_name);
            break;
        case BackupStatus::FAILED_TO_RESTORE:
            LOG_ERROR(log, "Failed to restore from {} {}", (info.internal ? "internal backup" : "backup"), info.backup_name);
            break;
        default:
            break;
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
