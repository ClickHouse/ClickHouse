#include <Backups/BackupRenamingConfig.h>
#include <Parsers/ASTBackupQuery.h>


namespace DB
{
using Kind = ASTBackupQuery::Kind;
using ElementType = ASTBackupQuery::ElementType;

void BackupRenamingConfig::setNewTableName(const DatabaseAndTableName & old_table_name, const DatabaseAndTableName & new_table_name)
{
    old_to_new_table_names[old_table_name] = new_table_name;
}

void BackupRenamingConfig::setNewDatabaseName(const String & old_database_name, const String & new_database_name)
{
    old_to_new_database_names[old_database_name] = new_database_name;
}

void BackupRenamingConfig::setNewTemporaryTableName(const String & old_temporary_table_name, const String & new_temporary_table_name)
{
    old_to_new_temporary_table_names[old_temporary_table_name] = new_temporary_table_name;
}

void BackupRenamingConfig::setFromBackupQuery(const ASTBackupQuery & backup_query)
{
    setFromBackupQueryElements(backup_query.elements);
}

void BackupRenamingConfig::setFromBackupQueryElements(const ASTBackupQuery::Elements & backup_query_elements)
{
    for (const auto & element : backup_query_elements)
    {
        switch (element.type)
        {
            case ElementType::TABLE: [[fallthrough]];
            case ElementType::DICTIONARY:
            {
                const auto & new_name = element.new_name.second.empty() ? element.name : element.new_name;
                setNewTableName(element.name, new_name);
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const auto & new_name = element.new_name.first.empty() ? element.name.first : element.new_name.first;
                setNewDatabaseName(element.name.first, new_name);
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                const auto & new_name = element.new_name.second.empty() ? element.name.second : element.new_name.second;
                setNewTemporaryTableName(element.name.second, new_name);
                break;
            }

            case ASTBackupQuery::ALL_DATABASES: break;
            case ASTBackupQuery::ALL_TEMPORARY_TABLES: break;
            case ASTBackupQuery::EVERYTHING: break;
        }
    }
}

DatabaseAndTableName BackupRenamingConfig::getNewTableName(const DatabaseAndTableName & old_table_name) const
{
    auto it = old_to_new_table_names.find(old_table_name);
    if (it != old_to_new_table_names.end())
        return it->second;
    return {getNewDatabaseName(old_table_name.first), old_table_name.second};
}

const String & BackupRenamingConfig::getNewDatabaseName(const String & old_database_name) const
{
    auto it = old_to_new_database_names.find(old_database_name);
    if (it != old_to_new_database_names.end())
        return it->second;
    return old_database_name;
}

const String & BackupRenamingConfig::getNewTemporaryTableName(const String & old_temporary_table_name) const
{
    auto it = old_to_new_temporary_table_names.find(old_temporary_table_name);
    if (it != old_to_new_temporary_table_names.end())
        return it->second;
    return old_temporary_table_name;
}

}
