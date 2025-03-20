#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// Executes OFFSET (without LIMIT). See OffsetTransform.
class OffsetStep : public ITransformingStep
{
public:
    OffsetStep(const Header & input_header_, size_t offset_);

    String getName() const override { return "Offset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t offset;
};

}
