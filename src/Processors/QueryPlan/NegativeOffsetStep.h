#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// Executes OFFSET (without LIMIT). See OffsetTransform.
class NegativeOffsetStep : public ITransformingStep
{
public:
    NegativeOffsetStep(const SharedHeader & input_header_, UInt64 offset_);

    String getName() const override { return "NegativeOffset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// A negative `OFFSET` skips rows of the entire query result, not of each shard, so like
    /// `OffsetStep` this step is evaluated on the initiator and a plan carrying one is otherwise as
    /// simple as any other. Reporting support keeps `considerEnablingParallelReplicas` from rejecting
    /// the whole plan (its check is a whole-plan gate). `transformPipeline` still attaches a collector,
    /// so that a boundary here would be measured rather than cached as `output_bytes = 0`.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    UInt64 offset;
};

}
