#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include "QueryPipeline/QueryPipelineBuilder.h"

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

class GroupingSetsStep : public ITransformingStep
{
public:
    GroupingSetsStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "GroupingSets"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    AggregatingTransformParamsPtr params;
};

}
