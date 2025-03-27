#pragma once

#include <Storages/StorageView.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

struct MetricOptions
{
    bool need_time{false};
    bool need_date{false};
    bool need_hostname{false};
};

class CustomMetricLogStep : public ITransformingStep
{
private:
    const HashMap<StringRef, size_t> & events_mapping;
    const HashMap<StringRef, size_t> & metrics_mapping;
    Names actual_events;
    Names actual_metrics;
    MetricOptions options;

public:
    CustomMetricLogStep(Block input_header_, Block output_header_, const HashMap<StringRef, size_t> & events_, const HashMap<StringRef, size_t> & metrics_, const Names & actual_events_, const Names & actual_metrics_, MetricOptions options_);

    String getName() const override
    {
        return "CustomMetricLogStep";
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void updateOutputHeader() override {}
};


class StorageSystemMetricLogView final : public IStorage
{
public:
    explicit StorageSystemMetricLogView(const StorageID & table_id_);

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

private:
    StorageView internal_view;
    HashMap<StringRef, size_t> events_mapping;
    HashMap<StringRef, size_t> metrics_mapping;
    size_t events_count;
    size_t metrics_count;

};

}
