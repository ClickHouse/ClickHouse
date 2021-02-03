#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public IQueryPlanStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    UnionStep(DataStreams input_streams_, Block result_header, size_t max_threads_ = 0);

    String getName() const override { return "Union"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    Block header;
    size_t max_threads;
    Processors processors;
};

}
