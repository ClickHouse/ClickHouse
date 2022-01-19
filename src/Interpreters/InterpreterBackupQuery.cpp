#include <Interpreters/InterpreterBackupQuery.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreTask.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/RestoreSettings.h>
#include <Backups/RestoreUtils.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace
{
    BackupMutablePtr createBackup(
            const BackupInfo & backup_info,
            IBackup::OpenMode open_mode,
            const BackupInfo * base_backup_info,
            const ContextPtr & context)
    {
        BackupFactory::CreateParams params;
        params.open_mode = open_mode;
        params.context = context;
        params.backup_info = backup_info;
        params.base_backup_info = base_backup_info ? *base_backup_info : std::optional<BackupInfo>{};
        return BackupFactory::instance().createBackup(params);
    }

    void executeBackup(const ContextPtr & context, const ASTBackupQuery & query)
    {
        auto backup_settings = BackupSettings::fromBackupQuery(query);
        BackupMutablePtr backup = createBackup(BackupInfo::fromAST(*query.backup_name), IBackup::OpenMode::WRITE, backup_settings.base_backup_info.get(), context);
        auto backup_entries = makeBackupEntries(context, query.elements, backup_settings);
        writeBackupEntries(backup, std::move(backup_entries), context->getSettingsRef().max_backup_threads);
    }

    void executeRestore(ContextMutablePtr context, const ASTBackupQuery & query)
    {
        auto restore_settings = RestoreSettings::fromRestoreQuery(query);
        BackupPtr backup = createBackup(BackupInfo::fromAST(*query.backup_name), IBackup::OpenMode::READ, restore_settings.base_backup_info.get(), context);
        auto restore_tasks = makeRestoreTasks(context, backup, query.elements, restore_settings);
        executeRestoreTasks(std::move(restore_tasks), context->getSettingsRef().max_backup_threads);
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    const auto & query = query_ptr->as<const ASTBackupQuery &>();
    if (query.kind == ASTBackupQuery::BACKUP)
        executeBackup(context, query);
    else if (query.kind == ASTBackupQuery::RESTORE)
        executeRestore(context, query);
    return {};
}

}
