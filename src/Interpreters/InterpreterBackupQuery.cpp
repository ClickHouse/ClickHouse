#include <Interpreters/InterpreterBackupQuery.h>
#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreTask.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupUtils.h>
#include <Backups/BackupsWorker.h>
#include <Backups/RestoreUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Common/Macros.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_IS_PROHIBITED;
}

namespace
{
    void checkNoMultipleReplicas(const std::vector<Strings> & cluster_host_ids, size_t only_shard_num)
    {
        if (only_shard_num)
        {
            if ((only_shard_num <= cluster_host_ids.size()) && (cluster_host_ids[only_shard_num - 1].size() > 1))
                throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Backup of multiple replicas is disabled, choose one replica with the replica_num setting or specify allow_storing_multiple_replicas=true");
        }
        for (size_t i = 0; i != cluster_host_ids.size(); ++i)
        {
            if (cluster_host_ids[i].size() > 1)
                throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Backup of multiple replicas is disabled, choose one replica with the replica_num setting or specify allow_storing_multiple_replicas=true");
        }
    }

    void executeBackupSync(const ASTBackupQuery & query, size_t task_id, const ContextPtr & context, const BackupInfo & backup_info, const BackupSettings & backup_settings, bool no_throw = false)
    {
        auto & worker = BackupsWorker::instance();
        bool is_internal_backup = backup_settings.internal;

        try
        {
            UUID backup_uuid = UUIDHelpers::generateV4();
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
            backup_create_params.coordination_zk_path = backup_settings.coordination_zk_path;

            ClusterPtr cluster;
            if (!query.cluster.empty())
            {
                new_query->cluster = context->getMacros()->expand(query.cluster);
                cluster = context->getCluster(new_query->cluster);
                auto new_backup_settings = backup_settings;
                new_backup_settings.cluster_host_ids = cluster->getHostIDs();
                if (!backup_settings.allow_storing_multiple_replicas && !backup_settings.replica_num)
                    checkNoMultipleReplicas(new_backup_settings.cluster_host_ids, backup_settings.shard_num);
                if (backup_settings.coordination_zk_path.empty())
                {
                    new_backup_settings.coordination_zk_path
                        = query.cluster.empty() ? "" : ("/clickhouse/backups/backup-" + toString(backup_uuid));
                    backup_create_params.coordination_zk_path = new_backup_settings.coordination_zk_path;
                }
                new_backup_settings.copySettingsToQuery(*new_query);
            }

            BackupMutablePtr backup = BackupFactory::instance().createBackup(backup_create_params);

            if (!query.cluster.empty())
            {
                if (!is_internal_backup)
                    worker.update(task_id, BackupStatus::MAKING_BACKUP);

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
                auto backup_entries = makeBackupEntries(context, new_query->elements, backup_settings);

                if (!is_internal_backup)
                    worker.update(task_id, BackupStatus::MAKING_BACKUP);

                writeBackupEntries(backup, std::move(backup_entries), context->getSettingsRef().max_backup_threads);
            }

            if (!is_internal_backup)
                worker.update(task_id, BackupStatus::BACKUP_COMPLETE);
        }
        catch (...)
        {
            if (!is_internal_backup)
                worker.update(task_id, BackupStatus::FAILED_TO_BACKUP, getCurrentExceptionMessage(false));
            if (!no_throw)
                throw;
        }
    }

    void executeRestoreSync(const ASTBackupQuery & query, size_t task_id, ContextMutablePtr context, const BackupInfo & backup_info, const RestoreSettings & restore_settings, bool no_throw = false)
    {
        auto & worker = BackupsWorker::instance();
        bool is_internal_restore = restore_settings.internal;

        try
        {
            std::shared_ptr<ASTBackupQuery> new_query = std::static_pointer_cast<ASTBackupQuery>(query.clone());

            ClusterPtr cluster;
            if (!query.cluster.empty())
            {
                new_query->cluster = context->getMacros()->expand(query.cluster);
                cluster = context->getCluster(new_query->cluster);
                auto new_restore_settings = restore_settings;
                new_restore_settings.cluster_host_ids = cluster->getHostIDs();
                if (new_restore_settings.coordination_zk_path.empty())
                {
                    UUID restore_uuid = UUIDHelpers::generateV4();
                    new_restore_settings.coordination_zk_path
                        = query.cluster.empty() ? "" : ("/clickhouse/backups/restore-" + toString(restore_uuid));
                }
                new_restore_settings.copySettingsToQuery(*new_query);
            }

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

                auto restore_tasks = makeRestoreTasks(context, backup, new_query->elements, restore_settings);

                executeRestoreTasks(std::move(restore_tasks), context->getSettingsRef().max_backup_threads);
            }

            if (!is_internal_restore)
                worker.update(task_id, BackupStatus::RESTORED);
        }
        catch (...)
        {
            if (!is_internal_restore)
                worker.update(task_id, BackupStatus::FAILED_TO_RESTORE, getCurrentExceptionMessage(false));
            if (!no_throw)
                throw;
        }
    }

    size_t executeBackup(const ASTBackupQuery & query, const ContextPtr & context)
    {
        const auto backup_info = BackupInfo::fromAST(*query.backup_name);
        const auto backup_settings = BackupSettings::fromBackupQuery(query);

        size_t task_id = 0;
        if (!backup_settings.internal)
            task_id = BackupsWorker::instance().add(backup_info.toString(), BackupStatus::PREPARING);

        if (backup_settings.async)
        {
            BackupsWorker::instance().run([query, task_id, context, backup_info, backup_settings]{ executeBackupSync(query, task_id, context, backup_info, backup_settings, /* no_throw = */ true); });
        }
        else
        {
            executeBackupSync(query, task_id, context, backup_info, backup_settings, /* no_throw = */ false);
        }
        return task_id;
    }

    size_t executeRestore(const ASTBackupQuery & query, ContextMutablePtr context)
    {
        const auto backup_info = BackupInfo::fromAST(*query.backup_name);
        const auto restore_settings = RestoreSettings::fromRestoreQuery(query);

        size_t task_id = 0;
        if (!restore_settings.internal)
            task_id = BackupsWorker::instance().add(backup_info.toString(), BackupStatus::RESTORING);

        if (restore_settings.async)
        {
            BackupsWorker::instance().run([query, task_id, context, backup_info, restore_settings]{ executeRestoreSync(query, task_id, context, backup_info, restore_settings, /* no_throw = */ true); });
        }
        else
        {
            executeRestoreSync(query, task_id, context, backup_info, restore_settings, /* no_throw = */ false);
        }
        return task_id;
    }

    Block getResultRow(size_t task_id)
    {
        if (!task_id)
            return {};
        auto entry = BackupsWorker::instance().getEntry(task_id);

        Block res_columns;

        auto column_task_id = ColumnUInt64::create();
        column_task_id->insert(task_id);
        res_columns.insert(0, {std::move(column_task_id), std::make_shared<DataTypeUInt64>(), "task_id"});

        auto column_backup_name = ColumnString::create();
        column_backup_name->insert(entry.backup_name);
        res_columns.insert(1, {std::move(column_backup_name), std::make_shared<DataTypeString>(), "backup_name"});

        auto column_status = ColumnInt8::create();
        column_status->insert(static_cast<Int8>(entry.status));
        res_columns.insert(2, {std::move(column_status), std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "status"});

        return res_columns;
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    const auto & query = query_ptr->as<const ASTBackupQuery &>();

    size_t task_id;
    if (query.kind == ASTBackupQuery::BACKUP)
        task_id = executeBackup(query, context);
    else
        task_id = executeRestore(query, context);

    BlockIO res_io;
    res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(getResultRow(task_id)));
    return res_io;
}

}
