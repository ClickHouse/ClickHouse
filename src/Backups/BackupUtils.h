#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Interpreters/Context_fwd.h>


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

/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements);

/// Checks the definition of a restored table - it must correspond to the definition from the backup.
bool compareRestoredTableDef(const IAST & restored_table_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context);
bool compareRestoredDatabaseDef(const IAST & restored_database_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context);

/// Returns true if this table should be skipped while making a backup because it's an inner table.
bool isInnerTable(const QualifiedTableName & table_name);
bool isInnerTable(const String & database_name, const String & table_name);

/// Returns the path in backup to the file storing a replicated table's metadata version,
/// placed next to the table's metadata file ("<table>.sql" -> "<table>.metadata_version.txt").
String getMetadataVersionPathInBackup(const String & table_metadata_path_in_backup);

}

}
