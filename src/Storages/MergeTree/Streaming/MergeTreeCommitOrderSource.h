#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/buildReadRoundPipeline.h>
#include <Storages/MergeTree/Streaming/PartitionsClassification.h>
#include <Storages/MergeTree/Streaming/ReadState.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Core/Streaming/Settings.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <Processors/IProcessor.h>

#include <optional>

namespace DB
{

/// Read-round loop streaming source.
class MergeTreeCommitOrderSource final : public IProcessor
{
    Status handleReadRoundStep();
    Status handleReadRoundShutdown();
    Status handleUpstreamShutdown();
    Status handleReconfiguration(const ClassifiedPartitions & partitions, bool subscription_updated);
    Status handleBoundedReconfiguration(const ClassifiedPartitions & partitions, bool subscription_updated);

    bool needToEmitGlobalIdle(const ClassifiedPartitions & partitions, bool subscription_updated);
    Status handleEmitGlobalIdle();

public:
    MergeTreeCommitOrderSource(
        SharedHeader header_,
        const MergeTreeData & storage_,
        const SelectQueryInfo & query_info_,
        ContextPtr context_,
        Names user_requested_columns_,
        size_t requested_num_streams_,
        UInt64 max_block_size_,
        MergeTreeBoundsSubscriptionPtr subscription_);

    String getName() const override { return "MergeTreeCommitOrderSource"; }

    Status prepare() override;
    void work() override;
    std::tuple<int, uint32_t, Int64> scheduleForEvent() override;
    PipelineUpdate updatePipeline() override;

    void onUpdatePorts() override;
    void onCancel() noexcept override;

private:
    const SharedHeader header;
    const MergeTreeBoundsSubscriptionPtr subscription;
    const StreamSettings stream_settings;
    const StorageLimitsListPtr storage_limits;
    const ReadRoundContext reading_context;
    const LoggerPtr log;

    /// Runtime information
    ReadState read_state;
    int64_t finished_rounds = 0;

    /// Reconfiguration
    std::optional<ReadRoundPipeline> current_round;
    std::optional<ReadRoundPipeline> pending_round;
};

}

#endif
