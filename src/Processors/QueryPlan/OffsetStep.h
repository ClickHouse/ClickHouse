#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// Executes OFFSET (without LIMIT). See OffsetTransform.
class OffsetStep : public ITransformingStep
{
public:
    OffsetStep(const SharedHeader & input_header_, size_t offset_);

    String getName() const override { return "Offset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    /// `considerEnablingParallelReplicas` uses this predicate as a whole-plan gate: a single
    /// unsupported step rejects the plan outright and no statistics are collected at all. `OFFSET`
    /// means skipping rows of the entire query result rather than of each shard, so the planner never
    /// pushes it below the replica boundary (see `apply_offset` in `Planner::buildQueryPlanIfNeeded`),
    /// and a plan carrying one is otherwise as simple as any other. Reporting support here is what lets
    /// `SELECT ... ORDER BY k OFFSET n` (an `OFFSET` without a `LIMIT`, the only shape that produces a
    /// bare `OffsetStep`) be considered for automatic parallel replicas at all.
    ///
    /// `transformPipeline` still attaches a `RuntimeDataflowStatisticsCollector` when instrumented.
    /// Being the replica-output boundary should be unreachable per the paragraph above, but an
    /// uninstrumented boundary fails open: it would cache `output_bytes = 0`, i.e. price the network
    /// transfer to the initiator at zero. Measuring the post-offset output instead fails close.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t offset;
};

}
