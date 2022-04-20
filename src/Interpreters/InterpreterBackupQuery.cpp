#include <Interpreters/InterpreterBackupQuery.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreTask.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/BackupsWorker.h>
#include <Backups/RestoreSettings.h>
#include <Backups/RestoreUtils.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{
namespace
{
    BackupMutablePtr createBackup(const BackupInfo & backup_info, const BackupSettings & backup_settings, const ContextPtr & context)
    {
        BackupFactory::CreateParams params;
        params.open_mode = IBackup::OpenMode::WRITE;
        params.context = context;
        params.backup_info = backup_info;
        params.base_backup_info = backup_settings.base_backup_info;
        params.compression_method = backup_settings.compression_method;
        params.compression_level = backup_settings.compression_level;
        params.password = backup_settings.password;
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

    void executeBackupSync(UInt64 task_id, const ContextPtr & context, const BackupInfo & backup_info, const ASTBackupQuery::Elements & backup_elements, const BackupSettings & backup_settings, bool no_throw = false)
    {
        auto & worker = BackupsWorker::instance();
        try
        {
            BackupMutablePtr backup = createBackup(backup_info, backup_settings, context);
            worker.update(task_id, BackupStatus::PREPARING);
            auto backup_entries = makeBackupEntries(context, backup_elements, backup_settings);
            worker.update(task_id, BackupStatus::MAKING_BACKUP);
            writeBackupEntries(backup, std::move(backup_entries), context->getSettingsRef().max_backup_threads);
            worker.update(task_id, BackupStatus::BACKUP_COMPLETE);
        }
        catch (...)
        {
            worker.update(task_id, BackupStatus::FAILED_TO_BACKUP, getCurrentExceptionMessage(false));
            if (!no_throw)
                throw;
        }
    }

    void executeRestoreSync(UInt64 task_id, ContextMutablePtr context, const BackupInfo & backup_info, const ASTBackupQuery::Elements & restore_elements, const RestoreSettings & restore_settings, bool no_throw = false)
    {
        auto & worker = BackupsWorker::instance();
        try
        {
            BackupPtr backup = openBackup(backup_info, restore_settings, context);
            worker.update(task_id, BackupStatus::RESTORING);
            auto restore_tasks = makeRestoreTasks(context, backup, restore_elements, restore_settings);
            executeRestoreTasks(std::move(restore_tasks), context->getSettingsRef().max_backup_threads);
            worker.update(task_id, BackupStatus::RESTORED);
        }
        catch (...)
        {
            worker.update(task_id, BackupStatus::FAILED_TO_RESTORE, getCurrentExceptionMessage(false));
            if (!no_throw)
                throw;
        }
    }

    UInt64 executeBackup(const ContextPtr & context, const ASTBackupQuery & query)
    {
        const auto backup_info = BackupInfo::fromAST(*query.backup_name);
        auto task_id = BackupsWorker::instance().add(backup_info.toString(), BackupStatus::PREPARING);
        const auto backup_settings = BackupSettings::fromBackupQuery(query);

        if (backup_settings.async)
        {
            ThreadFromGlobalPool thread{
                &executeBackupSync, task_id, context, backup_info, query.elements, backup_settings, /* no_throw = */ true};
            thread.detach(); /// TODO: Remove this !!! Move that thread to BackupsWorker instead
        }
        else
        {
            executeBackupSync(task_id, context, backup_info, query.elements, backup_settings, /* no_throw = */ false);
        }
        return task_id;
    }

    UInt64 executeRestore(ContextMutablePtr context, const ASTBackupQuery & query)
    {
        const auto backup_info = BackupInfo::fromAST(*query.backup_name);
        const auto restore_settings = RestoreSettings::fromRestoreQuery(query);
        auto task_id = BackupsWorker::instance().add(backup_info.toString(), BackupStatus::RESTORING);

        if (restore_settings.async)
        {
            ThreadFromGlobalPool thread{&executeRestoreSync, task_id, context, backup_info, query.elements, restore_settings, /* no_throw = */ true};
            thread.detach(); /// TODO: Remove this !!! Move that thread to BackupsWorker instead
        }
        else
        {
            executeRestoreSync(task_id, context, backup_info, query.elements, restore_settings, /* no_throw = */ false);
        }
        return task_id;
    }

    Block getResultRow(UInt64 task_id)
    {
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
    UInt64 task_id;
    if (query.kind == ASTBackupQuery::BACKUP)
        task_id = executeBackup(context, query);
    else
        task_id = executeRestore(context, query);

    BlockIO res_io;
    res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(getResultRow(task_id)));
    return res_io;
}

}
