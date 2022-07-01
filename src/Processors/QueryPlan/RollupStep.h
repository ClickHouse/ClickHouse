#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH ROLLUP. See RollupTransform.
class RollupStep : public ITransformingStep
{
public:
    RollupStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "Rollup"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    AggregatingTransformParamsPtr params;
};

}
