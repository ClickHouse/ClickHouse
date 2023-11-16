#pragma once

#include <Optimizer/PhysicalProperties.h>
#include <Optimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveOutputProp : public PlanStepVisitor<PhysicalProperties>
{
public:
    using Base = PlanStepVisitor<PhysicalProperties>;

    DeriveOutputProp(
        GroupNodePtr group_node_,
        const PhysicalProperties & required_prop_,
        const std::vector<PhysicalProperties> & children_prop_,
        ContextPtr context_);

    PhysicalProperties visit(QueryPlanStepPtr step) override;

    PhysicalProperties visitDefault(IQueryPlanStep & step) override;

    PhysicalProperties visit(ReadFromMergeTree & step) override;

    PhysicalProperties visit(SortingStep & step) override;

    PhysicalProperties visit(ExchangeDataStep & step) override;

    PhysicalProperties visit(ExpressionStep & step) override;

    PhysicalProperties visit(TopNStep & step) override;

    PhysicalProperties visit(UnionStep & step) override;

private:
    GroupNodePtr group_node;

    PhysicalProperties required_prop;
    std::vector<PhysicalProperties> children_prop;

    ContextPtr context;
};

}
