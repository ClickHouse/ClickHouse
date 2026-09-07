#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <unordered_set>
#include <vector>


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

/// Returns true if this table is an inner table by name, i.e. carries one of the reserved `.inner*`
/// prefixes. Usable wherever only names are available, in particular on the RESTORE path, where the
/// names come out of a backup and the tables they belong to need not exist.
bool isInnerTable(const QualifiedTableName & table_name);
bool isInnerTable(const String & database_name, const String & table_name);

/// Returns the names in `db_tables` which are inner tables of another table in the same set and so
/// must not be backed up in their own right. On top of `isInnerTable` this recognises inner tables
/// whose names carry no reserved prefix and can only be identified through the outer table owning
/// them.
///
/// The answer is derived from the create queries of `db_tables` alone, never from the live
/// `DatabaseCatalog`. `db_tables` is one enumeration of one database - for a `Replicated` database a
/// Keeper metadata snapshot, in which the outer table may not have been created on this replica yet
/// - so asking the catalog instead would make the classification depend on how far this replica has
/// caught up, and a lagging replica would back up a hidden table as a table of its own.
std::unordered_set<String> findInnerTables(const std::vector<std::pair<ASTPtr, StoragePtr>> & db_tables);

/// Returns true if a nested table of this name would belong to some other table, judged from the name
/// alone. This is only the shape of the name, never proof: an ordinary user table may carry it.
///
/// Its one use is to tell a caller that classifying this name needs the table which owns it, so that the
/// caller can make sure the enumeration it hands to `findInnerTables` contains that owner. Nothing decides
/// what a backup holds by this - `findInnerTables` does, against the definitions.
bool mayBeNestedTableName(const String & table_name);

}

}
