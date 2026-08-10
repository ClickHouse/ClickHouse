#pragma once

#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Core/Streaming/Settings.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <Processors/IProcessor.h>

#include <memory>

namespace DB
{

/// Snapshot-loop streaming source.
class MergeTreeCommitOrderSequentialSource final : public IProcessor
{
    Status handleRunningPipeline();
    Status handleReconfiguration();
    Status handleBoundedReconfiguration();
    void handlePipelineEnd();
    void surfaceFinalCursor();

public:
    MergeTreeCommitOrderSequentialSource(
        SharedHeader header_,
        const MergeTreeData & storage_,
        const SelectQueryInfo & query_info_,
        ContextPtr context_,
        Names user_requested_columns_,
        size_t requested_num_streams_,
        UInt64 max_block_size_,
        MergeTreeBoundsSubscriptionPtr subscription_);

    String getName() const override { return "MergeTreeCommitOrderSequentialSource"; }

    Status prepare() override;
    void work() override;
    int schedule() override;
    PipelineUpdate updatePipeline() override;

    void onUpdatePorts() override;
    void onCancel() noexcept override;

private:
    const SharedHeader header;
    const MergeTreeData & storage;
    const SelectQueryInfo query_info;
    const PrewhereInfoPtr initial_prewhere_info;
    const ContextPtr context;
    const Names user_requested_columns;
    const size_t requested_num_streams;
    const UInt64 max_block_size;
    const bool unordered;
    const MergeTreeBoundsSubscriptionPtr subscription;
    /// Streaming settings of the query (bounded flag, cursor, watermark); a query property read from `query_info`.
    const StreamSettings stream_settings;
    const LoggerPtr log;

    /// Query runtime information
    std::map<String, PartitionCursor> last_emitted_positions;

    /// Number of snapshots fully read so far (a metric; also gates a bounded stream's finish).
    size_t finished_snapshots = 0;

    /// Current snapshot runtime information
    Processors current_sub_pipeline;
    std::unique_ptr<QueryPlanResourceHolder> current_resources;
    std::map<String, Int64> reading_up_to_block_numbers;

    /// Reconfiguration
    std::optional<Pipe> pending_snapshot;
    std::unique_ptr<QueryPlanResourceHolder> pending_resources;
};

}
