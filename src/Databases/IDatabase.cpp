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

ASTPtr IDatabase::getCreateDatabaseQueryForBackup() const
{
    auto query = getCreateDatabaseQuery();
 
    /// We don't want to see any UUIDs in backup (after RESTORE the table will have another UUID anyway).
    auto & create = query->as<ASTCreateQuery &>();
    create.uuid = UUIDHelpers::Nil;

    return query;
}

DatabaseTablesIteratorPtr IDatabase::getTablesIteratorForBackup(const BackupEntriesCollector &) const
{
    /// IDatabase doesn't own any tables.
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void IDatabase::checkCreateTableQueryForBackup(const ASTPtr & create_table_query, const BackupEntriesCollector &) const
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} does not support backups, cannot backup table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

void IDatabase::createTableRestoredFromBackup(const ASTPtr & create_table_query, const RestorerFromBackup &)
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

}
