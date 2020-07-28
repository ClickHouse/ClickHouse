#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class ITransformingStep : public IQueryPlanStep
{
public:
    struct DataStreamTraits
    {
        bool preserves_distinct_columns;
    };

    ITransformingStep(DataStream input_stream, Block output_header, DataStreamTraits traits);

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

    virtual void transformPipeline(QueryPipeline & pipeline) = 0;
};

}
