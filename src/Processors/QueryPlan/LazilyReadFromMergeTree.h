#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

/// The second phase of lazy materialization optimization.
/// Reads lazy columns from MergeTree in `_part_starting_offset + _part_offset` order.
class LazilyReadFromMergeTree final : public ISourceStep
{
public:
    LazilyReadFromMergeTree(
        SharedHeader header,
        size_t max_block_size,
        size_t min_marks_for_concurrent_read_,
        MergeTreeReaderSettings reader_settings_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        const std::string & log_name_);

    void setLazyMaterializingRows(LazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazilyReadFromMergeTree"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    size_t max_block_size;
    size_t min_marks_for_concurrent_read;
    MergeTreeReaderSettings reader_settings;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const std::string log_name;

    LazyMaterializingRowsPtr lazy_materializing_rows;
};

}
