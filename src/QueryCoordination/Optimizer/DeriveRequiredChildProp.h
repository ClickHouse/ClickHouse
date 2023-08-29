#pragma once

#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveRequiredChildProp : public PlanStepVisitor<AlternativeChildrenProp>
{
public:
    explicit DeriveRequiredChildProp(GroupNode & group_node_) : group_node(group_node_) {}

    using Base = PlanStepVisitor<AlternativeChildrenProp>;

    AlternativeChildrenProp visit(QueryPlanStepPtr step) override;

    AlternativeChildrenProp visitDefault() override;

    AlternativeChildrenProp visit(ReadFromMergeTree & step) override;

    AlternativeChildrenProp visit(AggregatingStep & step) override;

    AlternativeChildrenProp visit(MergingAggregatedStep & step) override;

    AlternativeChildrenProp visit(ExpressionStep & step) override;

    AlternativeChildrenProp visit(SortingStep & step) override;

    AlternativeChildrenProp visit(LimitStep & step) override;

    AlternativeChildrenProp visit(JoinStep & step) override;

    AlternativeChildrenProp visit(ExchangeDataStep & step) override;

private:
    GroupNode & group_node;
};

}
