#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns final : public IStorage
{
public:
    explicit StorageSystemColumns(const StorageID & table_id_);

    std::string getName() const override { return "SystemColumns"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }
};


class ReadFromSystemColumns : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemColumns"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemColumns(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemColumns> storage_,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<StorageSystemColumns> storage;
    std::vector<UInt8> columns_mask;
    const size_t max_block_size;
    std::optional<ActionsDAG> virtual_columns_filter;
};


}
