#pragma once

#include <Databases/IDatabase.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

class Context;

namespace detail
{

ColumnPtr getFilteredDatabases(const ActionsDAG::Node * predicate, ContextPtr context);

/// The table names of `filtered_databases_column` that can pass `predicate`. `tables_filter` is
/// what the query asks of the table name column (`name` here, `table` for the detached tables),
/// and lets each database enumerate less than everything it holds.
ColumnPtr getFilteredTables(
    const ActionsDAG::Node * predicate,
    const ColumnPtr & filtered_databases_column,
    ContextPtr context,
    bool is_detached,
    const TablesFilter & tables_filter);

}


/** Implements the system table `tables`, which allows you to get information about all tables.
  */
class StorageSystemTables final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemTables(const StorageID & table_id_);

    std::string getName() const override { return "SystemTables"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
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
