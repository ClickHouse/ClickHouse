#pragma once

#include <Parsers/ASTBackupQuery.h>
#include <Core/Types.h>
#include <map>
#include <unordered_map>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/// Keeps information about renamings of databases or tables being processed
/// while we're making a backup or while we're restoring from a backup.
class BackupRenamingConfig
{
public:
    BackupRenamingConfig() = default;

    void setNewTableName(const DatabaseAndTableName & old_table_name, const DatabaseAndTableName & new_table_name);
    void setNewDatabaseName(const String & old_database_name, const String & new_database_name);

    void setFromBackupQuery(const ASTBackupQuery & backup_query, const String & current_database);
    void setFromBackupQueryElements(const ASTBackupQuery::Elements & backup_query_elements, const String & current_database);

    /// Changes names according to the renaming.
    DatabaseAndTableName getNewTableName(const DatabaseAndTableName & old_table_name) const;
    const String & getNewDatabaseName(const String & old_database_name) const;

private:
    std::map<DatabaseAndTableName, DatabaseAndTableName> old_to_new_table_names;
    std::unordered_map<String, String> old_to_new_database_names;
};

using BackupRenamingConfigPtr = std::shared_ptr<const BackupRenamingConfig>;

}
