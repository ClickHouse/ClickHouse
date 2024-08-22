#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;

namespace detail
{

ColumnPtr getFilteredDatabases(const ActionsDAG::Node * predicate, ContextPtr context);
ColumnPtr
getFilteredTables(const ActionsDAG::Node * predicate, const ColumnPtr & filtered_databases_column, ContextPtr context, bool is_detached);

}


/** Implements the system table `tables`, which allows you to get information about all tables.
  */
class StorageSystemTables final : public IStorage
{
public:
    explicit StorageSystemTables(const StorageID & table_id_);

    std::string getName() const override { return "SystemTables"; }

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
