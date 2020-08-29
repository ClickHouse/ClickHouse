#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH CUBE. See CubeTransform.
class CubeStep : public ITransformingStep
{
public:
    CubeStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "Cube"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    AggregatingTransformParamsPtr params;
};

}
