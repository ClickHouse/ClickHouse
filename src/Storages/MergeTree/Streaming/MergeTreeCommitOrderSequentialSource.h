#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/Streaming/CommitOrderReadStrategy.h>
#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>

namespace DB
{

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
        CommitOrderReadStrategyPtr read_strategy_,
        RangesInDataPartStreamSubscriptionPtr subscription_);

    String getName() const override { return "MergeTreeCommitOrderSequentialSource"; }

    Status prepare() override;
    void work() override;
    int schedule() override;
    PipelineUpdate updatePipeline() override;

    void onCancel() noexcept override;

private:
    const SharedHeader header;
    const CommitOrderReadStrategyPtr read_strategy;
    const RangesInDataPartStreamSubscriptionPtr subscription;
    const LoggerPtr log;

    std::list<RangesInDataPart> pending;
    Processors current_sub_pipeline;
};

}
