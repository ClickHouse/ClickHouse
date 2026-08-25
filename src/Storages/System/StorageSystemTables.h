#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

class Context;

namespace detail
{

struct FilteredDatabases
{
    ColumnPtr column;
    /// True when the query's predicate narrowed the database list, that is, the query named the
    /// databases it is interested in rather than scanning every one of them. A scan that named no
    /// database must not fail because a single database cannot list its tables (an unreachable
    /// `MySQL` / `PostgreSQL` remote); a query that named one must report that failure.
    bool narrowed_by_query = false;
};

FilteredDatabases getFilteredDatabases(const ActionsDAG::Node * predicate, ContextPtr context);
ColumnPtr getFilteredTables(
    const ActionsDAG::Node * predicate,
    const FilteredDatabases & filtered_databases,
    ContextPtr context,
    bool is_detached);

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
