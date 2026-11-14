#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>


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

/// Whether `storage` keeps its data on a storage policy marked `backup_metadata_only`. Only for the plain `MergeTree` family:
/// for a replicated or shared `MergeTree` table, or a view with an inner table on such a policy, it warns and returns false.
bool isDataOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage, const ContextPtr & context);

/// The same for a table that may not exist on this host: a `Replicated` database gives only the definition of a table
/// not created on this replica yet, and then the definition decides.
bool isDataOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage, const ASTPtr & create_table_query, const ContextPtr & context);

/// Whether a table created by `create_query` is on a storage policy marked `backup_metadata_only`.
bool isCreatedOnBackupMetadataOnlyStoragePolicy(const ASTCreateQuery & create_query, const ContextPtr & context);

}

}
