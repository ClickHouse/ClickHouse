#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Backups/BackupEntriesCollector.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_RESTORE_TABLE;
}

StoragePtr IDatabase::getTable(const String & name, ContextPtr context) const
{
    if (auto storage = tryGetTable(name, context))
        return storage;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(database_name), backQuoteIfNeed(name));
}

void IDatabase::adjustCreateDatabaseQueryForBackup(ASTPtr & create_database_query) const
{
    /// We don't want to see any UUIDs in backup.
    auto & create = create_database_query->as<ASTCreateQuery &>();
    create.uuid = UUIDHelpers::Nil;
}

DatabaseTablesIteratorPtr IDatabase::getTablesIteratorForBackup(const BackupEntriesCollector &) const
{
    /// IDatabase doesn't own any tables.
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void IDatabase::backupCreateTableQuery(BackupEntriesCollector &, const StoragePtr & storage, const ASTPtr &)
{
    /// Cannot backup any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} doesn't support storing tables to backup, cannot backup table {}",
                    getEngineName(), storage->getStorageID().getFullTableName());
}

void IDatabase::createTableRestoredFromBackup(const RestorerFromBackup &, const ASTPtr & create_table_query)
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));

}

}
