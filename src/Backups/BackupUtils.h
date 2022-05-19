#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Common/ThreadPool.h>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
struct BackupSettings;
class IBackupCoordination;
class AccessRightsElements;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Prepares backup entries.
BackupEntries makeBackupEntries(
    const ContextPtr & context,
    const ASTBackupQuery::Elements & elements,
    const BackupSettings & backup_settings,
    std::shared_ptr<IBackupCoordination> backup_coordination,
    std::chrono::seconds timeout_for_other_nodes_to_prepare = std::chrono::seconds::zero());

/// Write backup entries to an opened backup.
void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, ThreadPool & thread_pool);

/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements, const BackupSettings & backup_settings);

}
