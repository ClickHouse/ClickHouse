#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/types.h>

namespace DB
{

/// Executes Fractional OFFSET (without LIMIT). See FractionalOffsetTransform.
class FractionalOffsetStep : public ITransformingStep
{
public:
    FractionalOffsetStep(const SharedHeader & input_header_, Float64 fractional_offset_);

    String getName() const override { return "FractionalOffset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    Float64 fractional_offset;
};

}
