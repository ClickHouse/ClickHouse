#pragma once

#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveRequiredChildProp : public PlanStepVisitor<AlternativeChildrenProp>
{
public:
    explicit DeriveRequiredChildProp(GroupNodePtr group_node_, ContextPtr context_) : group_node(group_node_), context(context_) { }

    using Base = PlanStepVisitor<AlternativeChildrenProp>;

    AlternativeChildrenProp visit(QueryPlanStepPtr step) override;

    AlternativeChildrenProp visitDefault(IQueryPlanStep & step) override;

    AlternativeChildrenProp visit(ReadFromMergeTree & step) override;

    AlternativeChildrenProp visit(AggregatingStep & step) override;

    AlternativeChildrenProp visit(MergingAggregatedStep & step) override;

    AlternativeChildrenProp visit(ExpressionStep & step) override;

    AlternativeChildrenProp visit(SortingStep & step) override;

    AlternativeChildrenProp visit(LimitStep & step) override;

    AlternativeChildrenProp visit(JoinStep & step) override;

    AlternativeChildrenProp visit(ExchangeDataStep & step) override;

    AlternativeChildrenProp visit(CreatingSetStep & step) override;

    AlternativeChildrenProp visit(TopNStep & step) override;

    AlternativeChildrenProp visit(DistinctStep & step) override;

    AlternativeChildrenProp visit(CreatingSetsStep & step) override;

    AlternativeChildrenProp visit(FilterStep & step) override;

    AlternativeChildrenProp visit(UnionStep & step) override;

private:
    GroupNodePtr group_node;
    ContextPtr context;
};

}
