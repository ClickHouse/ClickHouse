#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

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

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    AggregatingTransformParamsPtr params;
};

}
