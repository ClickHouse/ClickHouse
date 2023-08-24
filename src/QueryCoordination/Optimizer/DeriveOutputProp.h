#pragma once

#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveOutputProp : public PlanStepVisitor<PhysicalProperties>
{
public:
    using Base = PlanStepVisitor<PhysicalProperties>;

    DeriveOutputProp(const PhysicalProperties & required_prop_, const std::vector<PhysicalProperties> & children_prop_)
        : required_prop(required_prop_), children_prop(children_prop_)
    {
    }

    PhysicalProperties visit(QueryPlanStepPtr step) override;

    PhysicalProperties visitDefault() override;

    PhysicalProperties visit(ReadFromMergeTree & step) override;

    PhysicalProperties visit(AggregatingStep & step) override;

    PhysicalProperties visit(MergingAggregatedStep & step) override;

    PhysicalProperties visit(SortingStep & step) override;

    PhysicalProperties visit(ExchangeDataStep & step) override;

private:
    PhysicalProperties required_prop;
    std::vector<PhysicalProperties> children_prop;
};

}
