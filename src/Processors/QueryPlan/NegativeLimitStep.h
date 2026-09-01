#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Executes Negative LIMIT. See NegativeLimitTransform.
class NegativeLimitStep : public ITransformingStep
{
public:
    NegativeLimitStep(
        const SharedHeader & input_header_,
        UInt64 limit_, UInt64 offset_,
        bool with_ties_ = false,
        SortDescription description_ = {});

    String getName() const override { return "NegativeLimit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimit() const { return limit; }

    void markAsShardLimit() { is_shard_limit = true; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

    /// Unlike the other variants this one can be pushed to the shard: `addPreliminaryLimitStep` calls
    /// `markAsShardLimit` on it, and `apply_prelimit` does not exclude a negative limit. So it may end
    /// up at the replica-output boundary, which is precisely why `transformPipeline` attaches a
    /// collector - reporting support without one would cache `output_bytes = 0` and price the transfer
    /// to the initiator at zero.
    ///
    /// `LIMIT -n` returns the *last* `n` rows, so a shard `NegativeLimit` is a bounded top-N taken from
    /// the tail: every replica keeps its own last `n` rows and ships all of them, and the initiator
    /// takes the last `n` of the merged result. Its output is therefore replicated rather than
    /// partitioned, and `considerEnablingParallelReplicas` prices it like `LimitStep`.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    UInt64 limit;
    UInt64 offset;
    bool with_ties;
    const SortDescription description;
    bool is_shard_limit = false;
};

}
