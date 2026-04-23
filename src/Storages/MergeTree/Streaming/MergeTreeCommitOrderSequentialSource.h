#pragma once

#include <Core/Block_fwd.h>
#include <Core/Names.h>
#include <Processors/IProcessor.h>
#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>
#include <Storages/StorageSnapshot.h>

#include <list>

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
        Names columns_to_read_,
        RangesInDataPartStreamSubscriptionPtr subscription_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeCommitOrderSequentialSource"; }

    Status prepare() override;
    PipelineUpdate updatePipeline() override;

    void onCancel() noexcept override;

private:
    bool fillPendingFromSubscription();

    SharedHeader header;
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    Names columns_to_read;
    RangesInDataPartStreamSubscriptionPtr subscription;
    ContextPtr context;

    std::list<RangesInDataPart> pending;
    Processors current_sub_pipeline;
    bool need_update_pipeline = true;
};

}
