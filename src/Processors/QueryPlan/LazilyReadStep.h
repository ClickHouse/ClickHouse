#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class LazilyReadStep : public ITransformingStep
{
public:
    LazilyReadStep(
        const DataStream & input_stream_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const ContextPtr & context_);

    String getName() const override { return "LazilyRead"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    LazilyReadInfoPtr lazily_read_info;
    ContextPtr context;
};

}
