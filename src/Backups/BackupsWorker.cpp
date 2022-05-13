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
    extern const int QUERY_IS_PROHIBITED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void executeBackupImpl(const ASTBackupQuery & query, const UUID & backup_uuid, const ContextPtr & context, ThreadPool & thread_pool)
    {
        const auto backup_info = BackupInfo::fromAST(*query.backup_name);
        auto backup_settings = BackupSettings::fromBackupQuery(query);

        std::shared_ptr<ASTBackupQuery> new_query = std::static_pointer_cast<ASTBackupQuery>(query.clone());

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

        ClusterPtr cluster;
        if (!query.cluster.empty())
        {
            new_query->cluster = context->getMacros()->expand(query.cluster);
            cluster = context->getCluster(new_query->cluster);
            backup_settings.cluster_host_ids = cluster->getHostIDs();
            if (backup_settings.coordination_zk_path.empty())
            {
                String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                backup_settings.coordination_zk_path
                    = query.cluster.empty() ? "" : (root_zk_path + "/backup-" + toString(backup_uuid));
                backup_settings.coordination_zk_path = backup_settings.coordination_zk_path;
            }
            backup_settings.copySettingsToQuery(*new_query);
        }

        std::shared_ptr<IBackupCoordination> backup_coordination;
        if (!backup_settings.coordination_zk_path.empty())
            backup_coordination = std::make_shared<BackupCoordinationDistributed>(backup_settings.coordination_zk_path, [context=context] { return context->getZooKeeper(); });
        else
            backup_coordination = std::make_shared<BackupCoordinationLocal>();
        backup_create_params.backup_coordination = backup_coordination;

        BackupMutablePtr backup = BackupFactory::instance().createBackup(backup_create_params);

        if (!query.cluster.empty())
        {
            DDLQueryOnClusterParams params;
            params.cluster = cluster;
            params.only_shard_num = backup_settings.shard_num;
            params.only_replica_num = backup_settings.replica_num;
            auto res = executeDDLQueryOnCluster(new_query, context, params);

            PullingPipelineExecutor executor(res.pipeline);
            Block block;
            while (executor.pull(block));

            backup->finalizeWriting();
        }
        else
        {
            new_query->setDatabase(context->getCurrentDatabase());
            auto timeout_for_preparing = std::chrono::seconds{context->getConfigRef().getUInt("backups.backup_prepare_timeout", 0)};
            auto backup_entries = makeBackupEntries(context, new_query->elements, backup_settings, backup_coordination, timeout_for_preparing);
            writeBackupEntries(backup, std::move(backup_entries), thread_pool);
        }
    }

    void executeRestoreImpl(const ASTBackupQuery & query, const UUID & restore_uuid, ContextMutablePtr context, ThreadPool & thread_pool)
    {
        const auto backup_info = BackupInfo::fromAST(*query.backup_name);
        auto restore_settings = RestoreSettings::fromRestoreQuery(query);
        bool is_internal_restore = restore_settings.internal;

        std::shared_ptr<IRestoreCoordination> restore_coordination;
        SCOPE_EXIT({
            if (!is_internal_restore && restore_coordination)
                restore_coordination->drop();
        });

        std::shared_ptr<ASTBackupQuery> new_query = std::static_pointer_cast<ASTBackupQuery>(query.clone());

        ClusterPtr cluster;
        if (!query.cluster.empty())
        {
            new_query->cluster = context->getMacros()->expand(query.cluster);
            cluster = context->getCluster(new_query->cluster);
            restore_settings.cluster_host_ids = cluster->getHostIDs();
            if (restore_settings.coordination_zk_path.empty())
            {
                String root_zk_path = context->getConfigRef().getString("backups.zookeeper_path", "/clickhouse/backups");
                restore_settings.coordination_zk_path = query.cluster.empty() ? "" : (root_zk_path + "/restore-" + toString(restore_uuid));
            }
            restore_settings.copySettingsToQuery(*new_query);
        }

        if (!restore_settings.coordination_zk_path.empty())
            restore_coordination = std::make_shared<RestoreCoordinationDistributed>(restore_settings.coordination_zk_path, [context=context] { return context->getZooKeeper(); });
        else
            restore_coordination = std::make_shared<RestoreCoordinationLocal>();

        if (!query.cluster.empty())
        {
            DDLQueryOnClusterParams params;
            params.cluster = cluster;
            params.only_shard_num = restore_settings.shard_num;
            params.only_replica_num = restore_settings.replica_num;
            auto res = executeDDLQueryOnCluster(new_query, context, params);

            PullingPipelineExecutor executor(res.pipeline);
            Block block;
            while (executor.pull(block));
        }
        else
        {
            new_query->setDatabase(context->getCurrentDatabase());

            BackupFactory::CreateParams backup_open_params;
            backup_open_params.open_mode = IBackup::OpenMode::READ;
            backup_open_params.context = context;
            backup_open_params.backup_info = backup_info;
            backup_open_params.base_backup_info = restore_settings.base_backup_info;
            backup_open_params.password = restore_settings.password;
            BackupPtr backup = BackupFactory::instance().createBackup(backup_open_params);

            auto timeout_for_restoring_metadata = std::chrono::seconds{context->getConfigRef().getUInt("backups.restore_metadata_timeout", 0)};
            auto restore_tasks = makeRestoreTasks(context, backup, new_query->elements, restore_settings, restore_coordination, timeout_for_restoring_metadata);
            restoreMetadata(restore_tasks, restore_settings, restore_coordination, timeout_for_restoring_metadata);
            restoreData(restore_tasks, thread_pool);
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
    UUID uuid = UUIDHelpers::generateV4();

    BackupInfo backup_info;
    BackupSettings backup_settings;
    {
        const ASTBackupQuery & backup_query = typeid_cast<const ASTBackupQuery &>(*query);
        backup_info = BackupInfo::fromAST(*backup_query.backup_name);
        backup_settings = BackupSettings::fromBackupQuery(backup_query);
    }

    {
        Info info;
        info.uuid = uuid;
        info.backup_name = backup_info.toString();
        info.status = BackupStatus::MAKING_BACKUP;
        info.status_changed_time = time(nullptr);
        info.internal = backup_settings.internal;
        std::lock_guard lock{infos_mutex};
        infos.emplace(uuid, std::move(info));
    }

    /// If we will make a backup in a separate thread we need to copy the current query context and use a query scope.
    ContextPtr job_context = backup_settings.async ? Context::createCopy(context) : context;
    bool use_query_scope = backup_settings.async;

    auto job = [this, query, job_context, use_query_scope, uuid]
    {
        try
        {
            std::optional<CurrentThread::QueryScope> query_scope;
            if (use_query_scope)
                query_scope.emplace(job_context);
            const ASTBackupQuery & backup_query = typeid_cast<const ASTBackupQuery &>(*query);
            executeBackupImpl(backup_query, uuid, job_context, backups_thread_pool);
            std::lock_guard lock{infos_mutex};
            auto & info = infos.at(uuid);
            info.status = BackupStatus::BACKUP_COMPLETE;
            info.status_changed_time = time(nullptr);
            LOG_INFO(log, "{} {} was created successfully", (info.internal ? "Internal backup" : "Backup"), info.backup_name);
        }
        catch (...)
        {
            std::lock_guard lock{infos_mutex};
            auto & info = infos.at(uuid);
            info.status = BackupStatus::FAILED_TO_BACKUP;
            info.status_changed_time = time(nullptr);
            info.error_message = getCurrentExceptionMessage(false);
            info.exception = std::current_exception();
            LOG_ERROR(log, "Failed to create {} {}", (info.internal ? "internal backup" : "backup"), info.backup_name);
        }
    };

    if (backup_settings.async)
    {
        backups_thread_pool.scheduleOrThrowOnError(job);
    }
    else
    {
        job();

        std::exception_ptr exception;
        {
            std::lock_guard lock{infos_mutex};
            auto & info = infos.at(uuid);
            if (info.status == BackupStatus::FAILED_TO_BACKUP)
                exception = info.exception;
        }

        if (exception)
            std::rethrow_exception(exception);
    }

    return uuid;
}

UUID BackupsWorker::startRestoring(const ASTPtr & query, ContextMutablePtr context)
{
    UUID uuid = UUIDHelpers::generateV4();

    BackupInfo backup_info;
    RestoreSettings restore_settings;
    {
        const ASTBackupQuery & restore_query = typeid_cast<const ASTBackupQuery &>(*query);
        backup_info = BackupInfo::fromAST(*restore_query.backup_name);
        restore_settings = RestoreSettings::fromRestoreQuery(restore_query);
    }

    {
        Info info;
        info.uuid = uuid;
        info.backup_name = backup_info.toString();
        info.status = BackupStatus::RESTORING;
        info.status_changed_time = time(nullptr);
        info.internal = restore_settings.internal;
        std::lock_guard lock{infos_mutex};
        infos.emplace(uuid, std::move(info));
    }

    /// If we will make a backup in a separate thread we need to copy the current query context and use a query scope.
    ContextMutablePtr job_context = restore_settings.async ? Context::createCopy(context) : context;
    bool use_query_scope = restore_settings.async;

    auto job = [this, query, job_context, use_query_scope, uuid]
    {
        try
        {
            std::optional<CurrentThread::QueryScope> query_scope;
            if (use_query_scope)
                query_scope.emplace(job_context);
            const ASTBackupQuery & restore_query = typeid_cast<const ASTBackupQuery &>(*query);
            executeRestoreImpl(restore_query, uuid, job_context, restores_thread_pool);
            std::lock_guard lock{infos_mutex};
            auto & info = infos.at(uuid);
            info.status = BackupStatus::RESTORED;
            info.status_changed_time = time(nullptr);
            LOG_INFO(log, "Restored from {} {} successfully", (info.internal ? "internal backup" : "backup"), info.backup_name);
        }
        catch (...)
        {
            std::lock_guard lock{infos_mutex};
            auto & info = infos.at(uuid);
            info.status = BackupStatus::FAILED_TO_RESTORE;
            info.status_changed_time = time(nullptr);
            info.error_message = getCurrentExceptionMessage(false);
            info.exception = std::current_exception();
            LOG_ERROR(log, "Failed to restore from {} {}", (info.internal ? "internal backup" : "backup"), info.backup_name);
        }
    };

    if (restore_settings.async)
    {
        restores_thread_pool.scheduleOrThrowOnError(job);
    }
    else
    {
        job();

        std::exception_ptr exception;
        {
            std::lock_guard lock{infos_mutex};
            auto & info = infos.at(uuid);
            if (info.status == BackupStatus::FAILED_TO_RESTORE)
                exception = info.exception;
        }

        if (exception)
            std::rethrow_exception(exception);
    }

    return uuid;
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
