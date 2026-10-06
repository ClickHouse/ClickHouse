#pragma once

#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>

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
    void handlePipelineEnd();

public:
    MergeTreeCommitOrderSequentialSource(
        SharedHeader header_,
        const MergeTreeData & storage_,
        const SelectQueryInfo & query_info_,
        ContextPtr context_,
        Names user_requested_columns_,
        size_t requested_num_streams_,
        UInt64 max_block_size_,
        MergeTreeBoundsSubscriptionPtr subscription_,
        MergeTreeCursor starting_positions_);

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
    const MergeTreeBoundsSubscriptionPtr subscription;
    const LoggerPtr log;

    /// Runtime information
    MergeTreeCursor last_emitted_positions;

    Processors current_sub_pipeline;
    std::unique_ptr<QueryPlanResourceHolder> current_resources;
    std::map<String, Int64> reading_up_to_block_numbers;

    std::optional<Pipe> pending_snapshot;
    std::unique_ptr<QueryPlanResourceHolder> pending_resources;
};

}
