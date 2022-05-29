#include <Databases/IDatabase.h>
#include <Backups/BackupEntriesCollector.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_BACKUP_DATABASE;
}

StoragePtr IDatabase::getTable(const String & name, ContextPtr context) const
{
    if (auto storage = tryGetTable(name, context))
        return storage;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(database_name), backQuoteIfNeed(name));
}

void IDatabase::backup(const ASTPtr & /* create_database_query */,
                       const std::unordered_set<String> & /* table_names */, bool /* all_tables */,
                       const std::unordered_set<String> & /* except_table_names */,
                       const std::unordered_map<String, ASTs> & /* partitions */,
                       std::shared_ptr<BackupEntriesCollector> /* backup_entries_collector */)
{
    throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE,
                    "Database engine {} does not support backups, cannot backup database {}",
                    getEngineName(), getDatabaseName());
}

void IDatabase::backupMetadata(const ASTPtr & create_database_query, std::shared_ptr<BackupEntriesCollector> backup_entries_collector)
{
    /// If `create_database_query` is null the backup won't contain this database's metadata.
    if (create_database_query)
        backup_entries_collector->addBackupEntryForCreateQuery(create_database_query);
}

void IDatabase::checkNoTablesToBackup(const std::unordered_set<String> & table_names) const
{
    if (!table_names.empty())
        throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                        "Database engine {} doesn't support storing tables to backup, cannot backup table {}.{}",
                        getEngineName(), getDatabaseName(), *table_names.begin());
}

}
