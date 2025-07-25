#pragma once

#include <Databases/IDatabase.h>
#include <Storages/StorageMaterializedMySQL.h>

namespace DB
{

/** MaterializedMySQL database table iterator
 *
 * The iterator returns different storage engine types depending on the visitor.
 * When MySQLSync thread accesses, it always returns MergeTree
 * Other cases always convert MergeTree to StorageMaterializedMySQL
 */
class DatabaseMaterializedTablesIterator final : public IDatabaseTablesIterator
{
public:
    void next() override { nested_iterator->next(); }

    bool isValid() const override { return nested_iterator->isValid(); }

    const String & name() const override { return nested_iterator->name(); }

    const StoragePtr & table() const override
    {
        StoragePtr storage = std::make_shared<StorageMaterializedMySQL>(nested_iterator->table(), database);
        return tables.emplace_back(storage);
    }

    UUID uuid() const override { return nested_iterator->uuid(); }

    DatabaseMaterializedTablesIterator(DatabaseTablesIteratorPtr nested_iterator_, const IDatabase * database_)
        : IDatabaseTablesIterator(database_->getDatabaseName()), nested_iterator(std::move(nested_iterator_)), database(database_)
    {
    }

private:
    mutable std::vector<StoragePtr> tables;
    DatabaseTablesIteratorPtr nested_iterator;
    const IDatabase * database;
};

}
