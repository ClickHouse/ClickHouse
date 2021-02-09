#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH CUBE. See CubeTransform.
class GroupingSetsStep : public ITransformingStep
{
public:
    GroupingSetsStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "Grouping Sets"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    AggregatingTransformParamsPtr params;
};

}
