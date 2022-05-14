#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Common/ThreadPool.h>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;
class IBackupEntry;
using BackupEntryPtr = std::unique_ptr<IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
struct BackupSettings;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Prepares backup entries.
BackupEntries makeBackupEntries(const ContextPtr & context, const ASTBackupQuery::Elements & elements, const BackupSettings & backup_settings);

/// Write backup entries to an opened backup.
void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, ThreadPool & thread_pool);

}
