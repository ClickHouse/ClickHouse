#pragma once

#include <Parsers/ASTBackupQuery.h>


namespace DB
{
class IBackup;
class AccessRightsElements;
class DDLRenamingMap;

/// Initializes a DDLRenamingMap from a BACKUP or RESTORE query.
DDLRenamingMap makeRenamingMapFromBackupQuery(const ASTBackupQuery::Elements & elements);


/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements);

}
