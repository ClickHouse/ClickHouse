#pragma once

#include <Databases/IDatabase.h>
#include <Storages/StorageMaterializeMySQL.h>

namespace DB
{

class DatabaseMaterializeTablesIterator final : public IDatabaseTablesIterator
{
public:
    virtual void next() { nested_iterator->next(); }

    virtual bool isValid() const { return nested_iterator->isValid(); }

    virtual const String & name() const { return nested_iterator->name(); }

    virtual const StoragePtr & table() const
    {
        StoragePtr storage = std::make_shared<StorageMaterializeMySQL>(nested_iterator->table());
        return tables.emplace_back(storage);
    }

    virtual UUID uuid() const { return nested_iterator->uuid(); }

    DatabaseMaterializeTablesIterator(DatabaseTablesIteratorPtr nested_iterator_) : nested_iterator(std::move(nested_iterator_))
    {}

private:
    mutable std::vector<StoragePtr> tables;
    DatabaseTablesIteratorPtr nested_iterator;

};

}
