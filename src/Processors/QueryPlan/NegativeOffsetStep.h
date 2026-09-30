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

    /// Like `OffsetStep`: a negative `OFFSET` applies to the whole result, so it runs on the initiator.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    UInt64 offset;
};

}
