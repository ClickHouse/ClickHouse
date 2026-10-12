#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Core/UUID.h>


namespace DB
{
class IBackup;
class AccessRightsElements;
class ASTCreateQuery;
class DDLRenamingMap;
struct QualifiedTableName;

namespace BackupUtils
{

/// Initializes a DDLRenamingMap from a BACKUP or RESTORE query.
DDLRenamingMap makeRenamingMap(const ASTBackupQuery::Elements & elements);

/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements);

/// Checks the definition of a restored table - it must correspond to the definition from the backup.
bool compareRestoredTableDef(const IAST & restored_table_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context);
bool compareRestoredDatabaseDef(const IAST & restored_database_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context);

/// Returns true if this table should be skipped while making a backup because it's an inner table.
bool isInnerTable(const QualifiedTableName & table_name);
bool isInnerTable(const String & database_name, const String & table_name);

/// A table on a storage policy marked `backup_metadata_only` gets its data from the storage itself, e.g. a shared
/// read-only dataset: `BACKUP` stores only the definition of the table, and `RESTORE` recreates the table with its UUID
/// from the backup, because the data is on the storage under a path derived from the UUID, and doesn't restore data.

/// Whether the data of `storage` is on storage policies marked `backup_metadata_only`. The data of a materialized view
/// or a `TimeSeries` table is in its inner tables. Throws if only a part of the data is on such policies.
bool isDataOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage, const ContextPtr & context);

/// For a table whose data is on such policies: the UUID of each table that keeps its data (the table itself, or the inner
/// tables of a materialized view or a `TimeSeries` table), paired with the UUID that table has in `create_query_from_backup`.
/// The data on the storage is the data of the backup only if they are equal.
std::vector<std::pair<UUID, UUID>> getUUIDsOfDataStorages(
    const StoragePtr & storage, const ASTCreateQuery & create_query_from_backup, const ContextPtr & context);

/// Whether a table created by `create_query`, or one of the inner tables of the view it creates, is on a storage policy
/// marked `backup_metadata_only`.
bool isCreatedOnBackupMetadataOnlyStoragePolicy(const ASTCreateQuery & create_query, const ContextPtr & context);

}

}
