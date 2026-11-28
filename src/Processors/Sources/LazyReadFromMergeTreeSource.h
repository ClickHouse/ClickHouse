#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Common/ListWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <optional>
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
    void work() override;

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
    VectorWithMemoryTracking<ListWithMemoryTracking<Chunk>> chunks;
    UnorderedMapWithMemoryTracking<const InputPort *, size_t> input_port_to_index;
    size_t next_chunk_to_process = 0;
    InputPorts::iterator next_input_to_process;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    /// Every range is read by one reader per column group; the readers of a range are consecutive inputs.
    size_t num_column_groups = 1;
    size_t num_ranges = 0;
    size_t next_range_to_process = 0;
    VectorWithMemoryTracking<InputPort *> input_ports;
    /// For every column of the output header: the column group that reads it and its position in that group's header.
    VectorWithMemoryTracking<std::pair<size_t, size_t>> output_column_sources;
    /// Joined by `work`, pushed by the next `prepare`.
    std::optional<Chunk> joined_chunk;

    Processors buildReaders();
    RangesInDataParts splitRanges(RangesInDataParts parts_with_ranges, size_t total_marks) const;
    VectorWithMemoryTracking<Names> splitColumnsIntoGroups(const RangesInDataParts & parts_with_ranges) const;
    Status prepareColumnGroups(const UpdatedInputPorts & updated_input_ports);
    Chunk joinColumnGroups(size_t range);
};

}
