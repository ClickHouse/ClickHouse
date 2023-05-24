#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class IntersectOrExceptStep : public IQueryPlanStep
{
using Operator = ASTSelectIntersectExceptQuery::Operator;

public:
    /// max_threads is used to limit the number of threads for result pipeline.
    IntersectOrExceptStep(DataStreams input_streams_, Operator operator_, size_t max_threads_ = 0);

    String getName() const override { return "IntersectOrExcept"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    Block header;
    Operator current_operator;
    size_t max_threads;
    Processors processors;
};

}
