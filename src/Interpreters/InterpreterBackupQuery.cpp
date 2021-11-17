#include <Interpreters/InterpreterBackupQuery.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace
{
    BackupMutablePtr createBackup(const ASTBackupQuery & query, const ContextPtr & context)
    {
        BackupFactory::CreateParams params;
        params.open_mode = (query.kind == ASTBackupQuery::BACKUP) ? IBackup::OpenMode::WRITE : IBackup::OpenMode::READ;
        params.context = context;

        params.backup_info = BackupInfo::fromAST(*query.backup_name);
        if (query.base_backup_name)
            params.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

        return BackupFactory::instance().createBackup(params);
    }

#if 0
    void getBackupSettings(const ASTBackupQuery & query, BackupSettings & settings, std::optional<BaseBackupInfo> & base_backup)
    {
        settings = {};
        if (query.settings)
            settings.applyChanges(query.settings->as<const ASTSetQuery &>().changes);
        return settings;
    }
#endif

    void executeBackup(const ASTBackupQuery & query, const ContextPtr & context)
    {
        BackupMutablePtr backup = createBackup(query, context);
        auto backup_entries = makeBackupEntries(query.elements, context);
        writeBackupEntries(backup, std::move(backup_entries), context->getSettingsRef().max_backup_threads);
    }

    void executeRestore(const ASTBackupQuery & query, ContextMutablePtr context)
    {
        BackupPtr backup = createBackup(query, context);
        auto restore_tasks = makeRestoreTasks(query.elements, context, backup);
        executeRestoreTasks(std::move(restore_tasks), context->getSettingsRef().max_backup_threads);
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    const auto & query = query_ptr->as<const ASTBackupQuery &>();
    if (query.kind == ASTBackupQuery::BACKUP)
        executeBackup(query, context);
    else if (query.kind == ASTBackupQuery::RESTORE)
        executeRestore(query, context);
    return {};
}

}
