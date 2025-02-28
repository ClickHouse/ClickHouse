#pragma once

#include <memory>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

class Context;
class StorageSystemReplicasImpl;

/** Implements `replicas` system table, which provides information about the status of the replicated tables.
  */
class StorageSystemReplicas final : public IStorage
{
public:
    explicit StorageSystemReplicas(const StorageID & table_id_);
    ~StorageSystemReplicas() override;

    std::string getName() const override { return "SystemReplicas"; }

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

private:
    std::shared_ptr<StorageSystemReplicasImpl> impl;
};


class ReadFromSystemReplicas : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemReplicas"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemReplicas(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::map<String, std::map<String, StoragePtr>> replicated_tables_,
        bool with_zk_fields_,
        size_t max_block_size_,
        std::shared_ptr<StorageSystemReplicasImpl> impl_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , replicated_tables(std::move(replicated_tables_))
        , with_zk_fields(with_zk_fields_)
        , max_block_size(max_block_size_)
        , impl(std::move(impl_))
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    const bool with_zk_fields;
    const size_t max_block_size;
    std::shared_ptr<StorageSystemReplicasImpl> impl;
    ExpressionActionsPtr virtual_columns_filter;
};

}
