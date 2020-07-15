#pragma once

#include <Databases/IDatabase.h>
#include <Storages/StorageMaterializeMySQL.h>

namespace DB
{

class DatabaseMaterializeTablesIterator final : public IDatabaseTablesIterator
{
public:
    void next() override { nested_iterator->next(); }

    bool isValid() const override { return nested_iterator->isValid(); }

    const String & name() const override { return nested_iterator->name(); }

    const StoragePtr & table() const override
    {
        StoragePtr storage = std::make_shared<StorageMaterializeMySQL>(nested_iterator->table());
        return tables.emplace_back(storage);
    }

    UUID uuid() const override { return nested_iterator->uuid(); }

    DatabaseMaterializeTablesIterator(DatabaseTablesIteratorPtr nested_iterator_) : nested_iterator(std::move(nested_iterator_))
    {}

private:
    mutable std::vector<StoragePtr> tables;
    DatabaseTablesIteratorPtr nested_iterator;

};

}
