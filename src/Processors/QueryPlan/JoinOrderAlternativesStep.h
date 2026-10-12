#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Join trees over the same relations that give the same result: first the order the join order optimizer
/// chose, then other ones it found. The Cascades optimizer puts all of them into one memo group and keeps the
/// cheapest one, so this step never reaches execution. Each input has its own copy of the relations below
/// the joins, with its own runtime filters, so the inputs that are not chosen leave no trace.
/// If the plan does not go through Cascades, `resolveJoinOrderAlternatives` keeps the first input.
class JoinOrderAlternativesStep : public IQueryPlanStep
{
public:
    /// The inputs, at least two, must have equal headers.
    explicit JoinOrderAlternativesStep(SharedHeaders input_headers_);

    String getName() const override { return "JoinOrderAlternatives"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;
};

}
