#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

/// Unordered version of LazyReadFromMergeTreeSource.
/// Creates a ReadFromMergeTree pipeline on expand and passes through chunks.
/// Used when row order doesn't matter (e.g. lazy FINAL branch).
class LazyUnorderedReadFromMergeTreeSource final : public IProcessor
{
public:
    LazyUnorderedReadFromMergeTreeSource(
        SharedHeader header,
        size_t max_block_size_,
        size_t max_threads_,
        MergeTreeReaderSettings reader_settings_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        const MergeTreeData & data_,
        ContextPtr context_,
        const std::string & log_name_,
        LazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazyUnorderedReadFromMergeTreeSource"; }
    Status prepare() override;
    PipelineUpdate updatePipeline() override;

private:
    size_t max_block_size;
    size_t max_threads;
    MergeTreeReaderSettings reader_settings;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    const MergeTreeData & data;
    ContextPtr context;
    const std::string log_name;
    LazyMaterializingRowsPtr lazy_materializing_rows;

    QueryPlanResourceHolder resources;

    Processors buildReaders();
};

}
