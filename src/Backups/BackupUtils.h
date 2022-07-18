#pragma once

#include <Parsers/ASTBackupQuery.h>


namespace DB
{
class IBackup;
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
void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, size_t num_threads);

/// Returns the path to metadata in backup.
String getMetadataPathInBackup(const DatabaseAndTableName & table_name);
String getMetadataPathInBackup(const String & database_name);
String getMetadataPathInBackup(const IAST & create_query);

/// Returns the path to table's data in backup.
String getDataPathInBackup(const DatabaseAndTableName & table_name);
String getDataPathInBackup(const IAST & create_query);

}
