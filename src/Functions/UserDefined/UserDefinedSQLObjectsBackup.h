#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <vector>

#include <Common/VectorWithMemoryTracking.h>


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
    const VectorWithMemoryTracking<std::pair<String, ASTPtr>> & objects);

/// Restores user-defined SQL objects from the backup.
VectorWithMemoryTracking<std::pair<String, ASTPtr>>
restoreUserDefinedSQLObjects(RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type);

/// With a replicated storage of the objects, tells the other hosts of a RESTORE ON CLUSTER which directory of the backup
/// this host would restore the objects from. Must be called before the stage barrier that precedes
/// `restoreUserDefinedSQLObjectsOfAllHosts`.
void registerUserDefinedSQLObjectsDirForRestore(
    RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type);

/// Reads the user-defined SQL objects to restore into the storage of this host. With a replicated storage, the one host
/// which acquires the restore reads the directories registered by every host sharing the storage (see
/// `registerUserDefinedSQLObjectsDirForRestore`): identical definitions of an object collapse into one, and different
/// definitions of the same object are rejected. `data_path_in_backup` is empty if this host's part of the backup has no
/// such objects. Returns false if nothing is restored into the storage of this host by any host. Otherwise returns true,
/// and `objects` are the ones this host has to store (none if another host stores them).
bool restoreUserDefinedSQLObjectsOfAllHosts(
    RestorerFromBackup & restorer,
    const String & data_path_in_backup,
    UserDefinedSQLObjectType object_type,
    VectorWithMemoryTracking<std::pair<String, ASTPtr>> & objects);
}
