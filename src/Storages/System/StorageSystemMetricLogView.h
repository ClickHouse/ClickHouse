#pragma once

#include <Storages/StorageView.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{


/// Special view for transposed representation of system.metric_log.
/// Can be used as compatibility layer, when you want to store transposed table, but your queries want wide table.
///
/// This view is not attached by default, it's attached by TransposedMetricLog, because
/// it depend on it.
class StorageSystemMetricLogView final : public IStorage
{
public:
    explicit StorageSystemMetricLogView(const StorageID & table_id_, const StorageID & source_storage_id);

    std::string getName() const override { return "SystemMetricLogView"; }

    bool isSystemStorage() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    std::optional<String> addFilterByMetricNameStep(QueryPlan & query_plan, const Names & column_names, ContextPtr context);
private:
    StorageView internal_view;
};


/// Unfortunately the logic of this view is not easy to express with
/// default SQL operators in effective way. All attempts with GROUP BY are terribly slow and resource consuming.
/// That is why it's not just a normal StorageView, but a custom pipeline on top of StorageView.
///
/// This is one of the steps of this custom pipeline. It's public because we need to allow
/// filter push down through it and it's possible only with custom code in filterPushDown.cpp
class CustomMetricLogStep : public ITransformingStep
{
public:
    CustomMetricLogStep(Block input_header_, Block output_header_);

    String getName() const override
    {
        return "CustomMetricLogStep";
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void updateOutputHeader() override {}
};

}
