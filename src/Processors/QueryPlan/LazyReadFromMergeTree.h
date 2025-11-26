#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;


class LazyReadFromMergeTree final : public ISourceStep
{
public:
    LazyReadFromMergeTree(
        SharedHeader header,
        size_t max_block_size,
        MergeTreeReaderSettings reader_settings_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        const std::string & log_name_);

    void setLazyMaterializingRows(LazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazyReadFromMergeTree"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    size_t max_block_size;
    MergeTreeReaderSettings reader_settings;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const std::string log_name;

    LazyMaterializingRowsPtr lazy_materializing_rows;
};

}
