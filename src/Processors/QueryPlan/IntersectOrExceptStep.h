#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class IntersectOrExceptStep : public IQueryPlanStep
{
public:
    using Operator = ASTSelectIntersectExceptQuery::Operator;

    /// max_threads is used to limit the number of threads for result pipeline.
    /// right_rows_estimate is an optional estimate of the right (accumulated) input's row count,
    /// used to pre-size the ALL counting map. 0 means unknown.
    IntersectOrExceptStep(SharedHeaders input_headers_, Operator operator_, size_t max_threads_ = 0, size_t right_rows_estimate_ = 0);

    String getName() const override { return "IntersectOrExcept"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    Operator current_operator;
    size_t max_threads;
    size_t right_rows_estimate;
};

}
