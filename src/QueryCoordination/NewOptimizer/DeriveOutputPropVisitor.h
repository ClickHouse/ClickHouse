#pragma once

#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <QueryCoordination/NewOptimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveOutputPropVisitor : public PlanStepVisitor<PhysicalProperties>
{
public:
    using Base = PlanStepVisitor<PhysicalProperties>;

    DeriveOutputPropVisitor(const PhysicalProperties & required_prop_, const std::vector<PhysicalProperties> & children_prop_)
        : required_prop(required_prop_), children_prop(children_prop_)
    {
    }

    PhysicalProperties visit(QueryPlanStepPtr step) override
    {
        return Base::visit(step);
    }

    PhysicalProperties visitStep() override
    {
        return {.distribution = {.type = children_prop[0].distribution.type}};
    }

    PhysicalProperties visit(ReadFromMergeTree & /*step*/) override
    {
        //    TODO sort_description by pk, DistributionType by distributed table
        PhysicalProperties res{.distribution = {.type = PhysicalProperties::DistributionType::Any}};
        return res;
    }
    
    PhysicalProperties visit(AggregatingStep & step) override
    {
        if (step.isFinal())
        {
            /// TODO Hashed by keys
            if (children_prop[0].distribution.type == PhysicalProperties::DistributionType::Hashed)
            {
                return {.distribution = {.type = PhysicalProperties::DistributionType::Hashed}};
            }
        }

        return {.distribution = {.type = PhysicalProperties::DistributionType::Any}};
    }

    PhysicalProperties visit(MergingAggregatedStep & /*step*/) override
    {
        /// TODO Hashed by keys
        return {.distribution = {.type = children_prop[0].distribution.type}};
    }

    PhysicalProperties visit(SortingStep & step) override
    {
        const auto & sort_description = step.getSortDescription();
        PhysicalProperties properties{.distribution = {.type = children_prop[0].distribution.type}, .sort_description = sort_description};
        return properties;
    }

    PhysicalProperties visit(ExchangeDataStep & step) override
    {
        return {.distribution = {.type = step.getDistributionType()}, .sort_description = step.getSortDescription()};
    }

private:
    PhysicalProperties required_prop;
    std::vector<PhysicalProperties> children_prop;
};

}
