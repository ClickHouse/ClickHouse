#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Common/ThreadPool.h>


namespace DB
{
class IBackup;
using BackupMutablePtr = std::shared_ptr<IBackup>;
class IBackupEntry;
using BackupEntries = std::vector<std::pair<String, std::shared_ptr<const IBackupEntry>>>;
using DataRestoreTasks = std::vector<std::function<void()>>;
class AccessRightsElements;
class DDLRenamingMap;

/// Initializes a DDLRenamingMap from a BACKUP or RESTORE query.
DDLRenamingMap makeRenamingMapFromBackupQuery(const ASTBackupQuery::Elements & elements);

/// Write backup entries to an opened backup.
void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, ThreadPool & thread_pool);

/// Run data restoring tasks which insert data to tables.
void restoreTablesData(DataRestoreTasks && tasks, ThreadPool & thread_pool);

/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements);

}
