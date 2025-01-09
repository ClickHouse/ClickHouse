#pragma once

#include <Storages/System/StorageSystemParts.h>


namespace DB
{

class StoragesDroppedInfoStream : public StoragesInfoStreamBase
{
public:
    StoragesDroppedInfoStream(const ActionsDAGPtr & filter, ContextPtr context);
protected:
    bool tryLockTable(StoragesInfo &) override
    {
        // we don't need to lock a dropped table
        return true;
    }
};

class Context;


/** Implements system table 'dropped_tables_parts' which allows to get information about data parts for dropped but not yet removed tables.
  */
class StorageSystemDroppedTablesParts final : public StorageSystemParts
{
public:
    explicit StorageSystemDroppedTablesParts(const StorageID & table_id) : StorageSystemParts(table_id) {}

    std::string getName() const override { return "SystemDroppedTablesParts"; }
protected:
    std::unique_ptr<StoragesInfoStreamBase> getStoragesInfoStream(const ActionsDAGPtr &, const ActionsDAGPtr & filter, ContextPtr context) override
    {
        return std::make_unique<StoragesDroppedInfoStream>(filter, context);
    }
};

}
