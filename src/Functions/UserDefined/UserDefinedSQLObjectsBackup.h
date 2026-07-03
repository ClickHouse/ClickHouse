#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>


namespace DB
{
class BackupEntriesCollector;
class RestorerFromBackup;
enum class UserDefinedSQLObjectType : uint8_t;
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;

/// Makes backup entries to backup user-defined SQL objects.
void backupUserDefinedSQLObjects(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    UserDefinedSQLObjectType object_type,
    const std::vector<std::pair<String, ASTPtr>> & objects);

/// Restores user-defined SQL objects from the backup.
std::vector<std::pair<String, ASTPtr>>
restoreUserDefinedSQLObjects(RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type);
}
