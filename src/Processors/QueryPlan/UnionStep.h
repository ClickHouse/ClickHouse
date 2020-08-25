#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class UnionStep : public IQueryPlanStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    UnionStep(DataStreams input_streams_, Block result_header, size_t max_threads_);

    String getName() const override { return "Union"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

private:
    Block header;
    size_t max_threads;
};

}
