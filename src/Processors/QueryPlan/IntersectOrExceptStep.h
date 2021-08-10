#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Parsers/ASTIntersectOrExcept.h>


namespace DB
{

class IntersectOrExceptStep : public IQueryPlanStep
{
using Operators = ASTIntersectOrExcept::Operators;

public:
    /// max_threads is used to limit the number of threads for result pipeline.
    IntersectOrExceptStep(DataStreams input_streams_, const Operators & operators_, size_t max_threads_ = 0);

    String getName() const override { return "IntersectOrExcept"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    Block checkHeaders(const DataStreams & input_streams_) const;

    Block header;
    Operators operators;
    size_t max_threads;
    Processors processors;
};

}
