#pragma once

#include <Interpreters/Context.h>
#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreeData;
struct RangesInDataPart;

/// Coordinator for one pipeline stream. Owns a subscription whose queue carries
/// `RangesInDataPart` items (one per part) to read in commit order. For each item
/// it splices a per-part sub-pipeline via `Status::UpdatePipeline`, forwards its
/// chunks to the coordinator's output, and tears the sub-pipeline down before
/// splicing the next one.
class MergeTreeCommitOrderSequentialSource final : public IProcessor
{
public:
    MergeTreeCommitOrderSequentialSource(
        SharedHeader header_,
        const MergeTreeData & storage_,
        StorageSnapshotPtr storage_snapshot_,
        RangesInDataPartStreamSubscriptionPtr subscription_,
        Names columns_to_read_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeCommitOrderSequentialSource"; }

    Status prepare() override;
    void work() override;
    int schedule() override;
    PipelineUpdate updatePipeline() override;

    void onCancel() noexcept override;

private:
    const SharedHeader header;
    const MergeTreeData & storage;
    const StorageSnapshotPtr storage_snapshot;
    const RangesInDataPartStreamSubscriptionPtr subscription;
    const Names columns_to_read;
    const ContextPtr context;
    const LoggerPtr log;

    std::list<RangesInDataPart> pending;
    Processors current_sub_pipeline;
};

}
