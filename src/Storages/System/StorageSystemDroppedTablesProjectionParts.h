#pragma once

#include <Storages/System/StorageSystemProjectionParts.h>
#include <Storages/System/StorageSystemDroppedTablesParts.h>


namespace DB
{

class Context;


/** Implements system table 'dropped_tables_projection_parts' which allows to get information about data parts for dropped but not yet removed tables.
  */
class StorageSystemDroppedTablesProjectionParts final : public StorageSystemProjectionParts
{
public:
    explicit StorageSystemDroppedTablesProjectionParts(const StorageID & table_id) : StorageSystemProjectionParts(table_id) {}

    std::string getName() const override { return "SystemDroppedTablesProjectionParts"; }
protected:
    std::unique_ptr<StoragesInfoStreamBase> getStoragesInfoStream(std::optional<ActionsDAG>, std::optional<ActionsDAG> filter, ContextPtr context) override
    {
        return std::make_unique<StoragesDroppedInfoStream>(std::move(filter), context);
    }
};

}
