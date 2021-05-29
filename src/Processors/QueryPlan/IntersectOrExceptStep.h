#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class IntersectOrExceptStep : public IQueryPlanStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    IntersectOrExceptStep(bool is_except_, DataStreams input_streams_, Block result_header, size_t max_threads_ = 0);

    String getName() const override { return is_except ? "Except" : "Intersect"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;
private:
    bool is_except;
    Block header;
    size_t max_threads;
    Processors processors;
};

}

