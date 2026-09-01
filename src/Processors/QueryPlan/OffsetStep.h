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
    /// unsupported step rejects the plan outright and no statistics are collected at all. Reporting
    /// support here is what lets a plan carrying an `OffsetStep` be considered for automatic parallel
    /// replicas at all - `SELECT ... ORDER BY k OFFSET n`, or the shard-side `OFFSET` that
    /// `addPreliminaryLimitStep` emits underneath a negative limit.
    ///
    /// `OFFSET` skips rows of the entire query result rather than of each shard, and there is no way to
    /// evaluate it per replica (splitting the offset between them would skip a different set of rows),
    /// so a bare one is applied on the initiator. Where the planner does place an `OffsetStep` on the
    /// shard it sits under a `NegativeLimitStep`, never as the topmost replica step, so this step is
    /// not expected to be the replica-output boundary itself.
    ///
    /// `transformPipeline` still attaches a `RuntimeDataflowStatisticsCollector` when instrumented,
    /// because an uninstrumented boundary fails open: it would cache `output_bytes = 0`, i.e. price the
    /// network transfer to the initiator at zero. Measuring the post-offset output instead fails close.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t offset;
};

}
