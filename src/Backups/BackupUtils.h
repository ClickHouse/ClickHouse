#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Interpreters/Context_fwd.h>

#include <optional>


namespace DB
{
class IBackup;
class AccessRightsElements;
class DDLRenamingMap;
struct QualifiedTableName;

namespace BackupUtils
{

/// Initializes a DDLRenamingMap from a BACKUP or RESTORE query.
DDLRenamingMap makeRenamingMap(const ASTBackupQuery::Elements & elements);

/// Whether a DATABASE or ALL element of the query selects the `system.named_collections` table.
bool wildcardSelectionCoversSystemNamedCollections(const ASTBackupQuery::Elements & elements);

/// Whether a TABLE element of the query names the `system.named_collections` table.
bool explicitSelectionCoversSystemNamedCollections(const ASTBackupQuery::Elements & elements);

/// Returns access required to execute BACKUP query. `backup_named_collections` is the value of the
/// backup setting of the same name; it decides whether selecting the `system.named_collections`
/// table requires the rights to read the collections' values (see `BackupSettings`).
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements, std::optional<bool> backup_named_collections);

/// Checks the definition of a restored table - it must correspond to the definition from the backup.
bool compareRestoredTableDef(const IAST & restored_table_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context);
bool compareRestoredDatabaseDef(const IAST & restored_database_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context);

/// Returns true if this table should be skipped while making a backup because it's an inner table.
bool isInnerTable(const QualifiedTableName & table_name);
bool isInnerTable(const String & database_name, const String & table_name);

}

}
