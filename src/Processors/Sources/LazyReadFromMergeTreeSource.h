#pragma once

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

/// Dynamically created readers from MergeTree based on LazyMaterializingRows.
class LazyReadFromMergeTreeSource final : public IProcessor
{
public:
    LazyReadFromMergeTreeSource(
        SharedHeader header,
        size_t max_block_size_,
        size_t max_threads_,
        size_t min_marks_for_concurrent_read_,
        ExpressionActionsSettings actions_settings_,
        MergeTreeReaderSettings reader_settings_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        const std::string & log_name_,
        LazyMaterializingRowsPtr lazy_materializing_rows_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);
    ~LazyReadFromMergeTreeSource() override;

    String getName() const override { return "LazyReadFromMergeTreeSource"; }
    Status prepare(const PortNumbers & updated_input_ports, const PortNumbers & /*updated_output_ports*/) override;
    Processors expandPipeline() override;

private:
    size_t max_block_size;
    size_t max_threads;
    size_t min_marks_for_concurrent_read;

    ExpressionActionsSettings actions_settings;
    MergeTreeReaderSettings reader_settings;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const std::string log_name;

    LazyMaterializingRowsPtr lazy_materializing_rows;
    std::vector<std::list<Chunk>> chunks;
    size_t next_chunk_to_process = 0;
    InputPorts::iterator next_input_to_process;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    Processors buildReaders();
    RangesInDataParts splitRanges(RangesInDataParts parts_with_ranges, size_t total_marks) const;
};

}
