#pragma once

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <unordered_map>

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
    Status prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & /*updated_output_ports*/) override;
    PipelineUpdate updatePipeline() override;

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
    std::unordered_map<const InputPort *, size_t> input_port_to_index;
    size_t next_chunk_to_process = 0;
    InputPorts::iterator next_input_to_process;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    Processors buildReaders();
    /// Point-read fast path: when the lazy read carries a vector column with a `Quantized(...)` codec that every part
    /// stores one vector per compressed block, fetch each shortlisted row's single block for that column instead of
    /// decompressing whole granules; the other lazy columns are read normally alongside it and merged into the same
    /// chunk. Returns the sources when the whole read qualifies, or nothing to fall back on the granule read.
    /// See MergeTreePointReadSource.
    Processors tryBuildPointReadSources();
    RangesInDataParts splitRanges(RangesInDataParts parts_with_ranges, size_t total_marks) const;
};

}
