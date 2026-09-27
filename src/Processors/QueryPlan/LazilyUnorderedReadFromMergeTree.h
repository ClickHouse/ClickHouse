#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

/// Unordered lazy column reader for lazy FINAL optimization.
/// Unlike LazilyReadFromMergeTree, this does not preserve row order —
/// it creates a ReadFromMergeTree internally and passes through chunks.
class LazilyUnorderedReadFromMergeTree final : public ISourceStep
{
public:
    LazilyUnorderedReadFromMergeTree(
        SharedHeader header,
        size_t max_block_size_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        const MergeTreeData & data_,
        ContextPtr context_,
        const std::string & log_name_);

    void setLazyMaterializingRows(LazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazilyReadFromMergeTree"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    size_t max_block_size;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    const MergeTreeData & data;
    ContextPtr context;
    const std::string log_name;

    LazyMaterializingRowsPtr lazy_materializing_rows;
};

}
