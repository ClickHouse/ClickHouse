#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class ITransformingStep : public IQueryPlanStep
{
public:
    ITransformingStep(DataStream input_stream, DataStream output_stream);

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

    virtual void transformPipeline(QueryPipeline & pipeline) = 0;
};

}
