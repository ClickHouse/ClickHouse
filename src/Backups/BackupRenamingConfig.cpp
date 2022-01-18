#include <Backups/BackupRenamingConfig.h>
#include <Parsers/ASTBackupQuery.h>
#include <Interpreters/DatabaseCatalog.h>


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

void BackupRenamingConfig::setFromBackupQuery(const ASTBackupQuery & backup_query, const String & current_database)
{
    setFromBackupQueryElements(backup_query.elements, current_database);
}

void BackupRenamingConfig::setFromBackupQueryElements(const ASTBackupQuery::Elements & backup_query_elements, const String & current_database)
{
    for (const auto & element : backup_query_elements)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                const String & table_name = element.name.second;
                String database_name = element.name.first;
                if (element.name_is_in_temp_db)
                    database_name = DatabaseCatalog::TEMPORARY_DATABASE;
                else if (database_name.empty())
                    database_name = current_database;

                const String & new_table_name = element.new_name.second;
                String new_database_name = element.new_name.first;
                if (element.new_name_is_in_temp_db)
                    new_database_name = DatabaseCatalog::TEMPORARY_DATABASE;
                else if (new_database_name.empty())
                    new_database_name = current_database;

                setNewTableName({database_name, table_name}, {new_database_name, new_table_name});
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                String database_name = element.name.first;
                if (element.name_is_in_temp_db)
                    database_name = DatabaseCatalog::TEMPORARY_DATABASE;

                String new_database_name = element.new_name.first;
                if (element.new_name_is_in_temp_db)
                    new_database_name = DatabaseCatalog::TEMPORARY_DATABASE;

                setNewDatabaseName(database_name, new_database_name);
                break;
            }

            case ASTBackupQuery::ALL_DATABASES: break;
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

}
