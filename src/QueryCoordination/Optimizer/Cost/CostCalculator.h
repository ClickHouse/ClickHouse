#pragma once

#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>

namespace DB
{

class CostCalculator : public PlanStepVisitor<Float64>
{
public:
    using Base = PlanStepVisitor<Float64>;

    CostCalculator(const Statistics & statistics_, const std::vector<Statistics> & input_statistics_ = {}, const ChildrenProp & child_prop_ = {})
        : statistics(statistics_), input_statistics(input_statistics_), child_prop(child_prop_)
    {
    }

    Float64 visit(QueryPlanStepPtr step) override;

    Float64 visitDefault(IQueryPlanStep & step) override;

    Float64 visit(ReadFromMergeTree & step) override;

    Float64 visit(AggregatingStep & step) override;

    Float64 visit(MergingAggregatedStep & step) override;

    Float64 visit(ExchangeDataStep & step) override;

    Float64 visit(SortingStep & step) override;

    Float64 visit(JoinStep & step) override;

    Float64 visit(LimitStep & step) override;

    Float64 visit(TopNStep & step) override;

private:
    const Statistics & statistics;
    const std::vector<Statistics> & input_statistics;
    const ChildrenProp & child_prop;
};

}
