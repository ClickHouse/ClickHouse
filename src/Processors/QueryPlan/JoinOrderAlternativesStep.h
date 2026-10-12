#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Two join trees over the same relations that give the same result: the order the join order optimizer
/// chose and another one it found. The Cascades optimizer puts both into one memo group and keeps the
/// cheaper one, so this step never reaches execution. Each input has its own copy of the relations
/// below the joins, with its own runtime filters, so the input that is not chosen leaves no trace.
/// If the plan does not go through Cascades, `resolveJoinOrderAlternatives` keeps the first input.
class JoinOrderAlternativesStep : public IQueryPlanStep
{
public:
    /// The inputs must have equal headers.
    explicit JoinOrderAlternativesStep(SharedHeaders input_headers_);

    String getName() const override { return "JoinOrderAlternatives"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;
};

}
