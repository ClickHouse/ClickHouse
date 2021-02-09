#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

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

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    AggregatingTransformParamsPtr params;
};

}
