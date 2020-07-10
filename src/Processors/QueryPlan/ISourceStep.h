#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class ISourceStep : public IQueryPlanStep
{
public:
    explicit ISourceStep(DataStream output_stream_);

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

    virtual void initializePipeline(QueryPipeline & pipeline) = 0;
};

}
