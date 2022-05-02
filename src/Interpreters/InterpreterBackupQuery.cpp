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
namespace
{
    BackupMutablePtr createBackup(const UUID & backup_uuid, const BackupInfo & backup_info, const BackupSettings & backup_settings, const ContextPtr & context)
    {
        BackupFactory::CreateParams params;
        params.open_mode = IBackup::OpenMode::WRITE;
        params.context = context;
        params.backup_info = backup_info;
        params.base_backup_info = backup_settings.base_backup_info;
        params.compression_method = backup_settings.compression_method;
        params.compression_level = backup_settings.compression_level;
        params.password = backup_settings.password;
        params.backup_uuid = backup_uuid;
        params.is_internal_backup = backup_settings.internal;
        params.coordination_zk_path = backup_settings.coordination_zk_path;
        return BackupFactory::instance().createBackup(params);
    }

    BackupMutablePtr openBackup(const BackupInfo & backup_info, const RestoreSettings & restore_settings, const ContextPtr & context)
    {
        BackupFactory::CreateParams params;
        params.open_mode = IBackup::OpenMode::READ;
        params.context = context;
        params.backup_info = backup_info;
        params.base_backup_info = restore_settings.base_backup_info;
        params.password = restore_settings.password;
        return BackupFactory::instance().createBackup(params);
    }

    void executeBackupSync(const ASTBackupQuery & query, size_t task_id, const ContextPtr & context, const BackupInfo & backup_info, const BackupSettings & backup_settings, bool no_throw = false)
    {
        auto & worker = BackupsWorker::instance();
        bool is_internal_backup = backup_settings.internal;

        try
        {
            UUID backup_uuid = UUIDHelpers::generateV4();

            auto new_backup_settings = backup_settings;
            if (!query.cluster.empty() && backup_settings.coordination_zk_path.empty())
                new_backup_settings.coordination_zk_path = query.cluster.empty() ? "" : ("/clickhouse/backups/backup-" + toString(backup_uuid));
            std::shared_ptr<ASTBackupQuery> new_query = std::static_pointer_cast<ASTBackupQuery>(query.clone());
            new_backup_settings.copySettingsToBackupQuery(*new_query);

            BackupMutablePtr backup = createBackup(backup_uuid, backup_info, new_backup_settings, context);

            if (!query.cluster.empty())
            {
                if (!is_internal_backup)
                    worker.update(task_id, BackupStatus::MAKING_BACKUP);

                DDLQueryOnClusterParams params;
                params.shard_index = new_backup_settings.shard_num;
                params.replica_index = new_backup_settings.replica_num;
                params.allow_multiple_replicas = new_backup_settings.allow_storing_multiple_replicas;
                auto res = executeDDLQueryOnCluster(new_query, context, params);

                PullingPipelineExecutor executor(res.pipeline);
                Block block;
                while (executor.pull(block));

                backup->finalizeWriting();
            }
            else
            {
                auto backup_entries = makeBackupEntries(context, new_query->elements, new_backup_settings);

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
            BackupPtr backup = openBackup(backup_info, restore_settings, context);

            auto new_restore_settings = restore_settings;
            if (!query.cluster.empty() && new_restore_settings.coordination_zk_path.empty())
            {
                UUID restore_uuid = UUIDHelpers::generateV4();
                new_restore_settings.coordination_zk_path
                    = query.cluster.empty() ? "" : ("/clickhouse/backups/restore-" + toString(restore_uuid));
            }
            std::shared_ptr<ASTBackupQuery> new_query = std::static_pointer_cast<ASTBackupQuery>(query.clone());
            new_restore_settings.copySettingsToRestoreQuery(*new_query);

            if (!query.cluster.empty())
            {
                DDLQueryOnClusterParams params;
                params.shard_index = new_restore_settings.shard_num;
                params.replica_index = new_restore_settings.replica_num;
                auto res = executeDDLQueryOnCluster(new_query, context, params);

                PullingPipelineExecutor executor(res.pipeline);
                Block block;
                while (executor.pull(block));
            }
            else
            {
                auto restore_tasks = makeRestoreTasks(context, backup, new_query->elements, new_restore_settings);
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
