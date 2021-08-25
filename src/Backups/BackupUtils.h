#pragma once

#include <Parsers/ASTBackupQuery.h>


namespace DB
{

class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;
class IBackupEntry;
using BackupEntryPtr = std::unique_ptr<IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
using RestoreDataTask = std::function<void()>;
using RestoreDataTasks = std::vector<RestoreDataTask>;
using RestoreObjectTask = std::function<RestoreDataTasks()>;
using RestoreObjectsTasks = std::vector<RestoreObjectTask>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
using ContextMutablePtr = std::shared_ptr<Context>;


/// Prepares backup entries.
BackupEntries makeBackupEntries(const ASTBackupQuery::Elements & elements, const ContextPtr & context);

/// Estimate total size of the backup which would be written from the specified entries.
UInt64 estimateBackupSize(const BackupEntries & backup_entries, const BackupPtr & base_backup);

/// Write backup entries to an opened backup.
void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, size_t num_threads);

/// Prepare restore tasks.
RestoreObjectsTasks makeRestoreTasks(const ASTBackupQuery::Elements & elements, ContextMutablePtr context, const BackupPtr & backup);

/// Execute restore tasks.
void executeRestoreTasks(RestoreObjectsTasks && restore_tasks, size_t num_threads);

}
