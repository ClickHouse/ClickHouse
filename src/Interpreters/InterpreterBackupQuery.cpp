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
    BackupSettings getBackupSettings(const ASTBackupQuery & query)
    {
        BackupSettings settings;
        if (query.settings)
            settings.applyChanges(query.settings->as<const ASTSetQuery &>().changes);
        return settings;
    }

    BackupPtr getBaseBackup(const BackupSettings & settings)
    {
        const String & base_backup_name = settings.base_backup;
        if (base_backup_name.empty())
            return nullptr;
        return BackupFactory::instance().openBackup(base_backup_name);
    }

    void executeBackup(const ASTBackupQuery & query, const ContextPtr & context)
    {
        auto settings = getBackupSettings(query);
        auto base_backup = getBaseBackup(settings);

        auto backup_entries = makeBackupEntries(query.elements, context);
        UInt64 estimated_backup_size = estimateBackupSize(backup_entries, base_backup);

        auto backup = BackupFactory::instance().createBackup(query.backup_name, estimated_backup_size, base_backup);
        writeBackupEntries(backup, std::move(backup_entries), context->getSettingsRef().max_backup_threads);
    }

    void executeRestore(const ASTBackupQuery & query, ContextMutablePtr context)
    {
        auto settings = getBackupSettings(query);
        auto base_backup = getBaseBackup(settings);

        auto backup = BackupFactory::instance().openBackup(query.backup_name, base_backup);
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
