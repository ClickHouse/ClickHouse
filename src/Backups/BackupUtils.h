#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Common/ThreadPool.h>


namespace DB
{
class IBackup;
using BackupMutablePtr = std::shared_ptr<IBackup>;
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
struct BackupSettings;
struct RestoreSettings;
using DataRestoreTask = std::function<void()>;
using DataRestoreTasks = std::vector<DataRestoreTask>;
class AccessRightsElements;

/// Write backup entries to an opened backup.
void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, ThreadPool & thread_pool);

/// Run data restoring tasks which insert data to tables.
void restoreTablesData(DataRestoreTasks && tasks, ThreadPool & thread_pool);

/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements, const BackupSettings & backup_settings);

/// Returns access required to execute RESTORE query.
AccessRightsElements getRequiredAccessToRestore(const ASTBackupQuery::Elements & elements, const RestoreSettings & restore_settings);

}
