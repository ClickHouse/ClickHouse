#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>


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

class ReadFromSystemTables : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemTables"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemTables(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::vector<UInt8> columns_mask;
    size_t max_block_size;

    ColumnPtr filtered_databases_column;
    ColumnPtr filtered_tables_column;
};

}
