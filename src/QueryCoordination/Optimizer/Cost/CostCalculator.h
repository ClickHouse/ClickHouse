#pragma once

#include <QueryCoordination/Optimizer/Cost/Cost.h>

#include <Interpreters/Cluster.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

/// Calculate cost for an single step.
class CostCalculator : public PlanStepVisitor<Cost>
{
public:
    using Base = PlanStepVisitor<Cost>;
    using ResultType = Cost;

    CostCalculator(
        const Statistics & statistics_,
        ContextPtr context_,
        const std::vector<Statistics> & input_statistics_ = {},
        const ChildrenProp & child_props_ = {})
        : statistics(statistics_), input_statistics(input_statistics_), child_props(child_props_), context(context_)
    {
        auto query_coordination_info = context->getQueryCoordinationMetaInfo();
        auto cluster = context->getCluster(query_coordination_info.cluster_name);
        node_count = cluster->getShardCount();
    }

    Cost visit(QueryPlanStepPtr step) override;

    Cost visitDefault(IQueryPlanStep & step) override;

    Cost visit(ReadFromMergeTree & step) override;

    Cost visit(AggregatingStep & step) override;

    Cost visit(MergingAggregatedStep & step) override;

    Cost visit(ExpressionStep & step) override;

    Cost visit(FilterStep & step) override;

    Cost visit(SortingStep & step) override;

    Cost visit(LimitStep & step) override;

    Cost visit(JoinStep & step) override;

    Cost visit(UnionStep & step) override;

    Cost visit(ExchangeDataStep & step) override;

    Cost visit(CreatingSetStep & step) override;

    Cost visit(ExtremesStep & step) override;

    Cost visit(RollupStep & step) override;

    Cost visit(CubeStep & step) override;

    Cost visit(TotalsHavingStep & step) override;

    Cost visit(TopNStep & step) override;

private:
    /// Output statistics of step
    const Statistics & statistics;

    /// Input statistics of step
    const std::vector<Statistics> & input_statistics;

    /// Required children steps physical properties(distribution)
    [[maybe_unused]] const ChildrenProp & child_props;

    /// Query context
    ContextPtr context;

    /// node count which participating the query.
    size_t node_count;
};

}
