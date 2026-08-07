#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;

/** Implements the system table `detached_tables`, which allows you to get information about detached tables.
  */
class StorageSystemDetachedTables final : public IStorage
{
public:
    explicit StorageSystemDetachedTables(const StorageID & table_id_);

    std::string getName() const override { return "SystemDetachedTables"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /*query_info*/,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }
};
}
