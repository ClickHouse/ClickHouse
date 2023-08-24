#include <QueryCoordination/Optimizer/DeriveOutputProp.h>

namespace DB
{

PhysicalProperties DeriveOutputProp::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

PhysicalProperties DeriveOutputProp::visitDefault()
{
    return {.distribution = {.type = children_prop[0].distribution.type}};
}

PhysicalProperties DeriveOutputProp::visit(ReadFromMergeTree & /*step*/)
{
    //    TODO sort_description by pk, DistributionType by distributed table
    PhysicalProperties res{.distribution = {.type = PhysicalProperties::DistributionType::Any}};
    return res;
}

PhysicalProperties DeriveOutputProp::visit(AggregatingStep & step)
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

PhysicalProperties DeriveOutputProp::visit(MergingAggregatedStep & /*step*/)
{
    /// TODO Hashed by keys
    return {.distribution = {.type = children_prop[0].distribution.type}};
}

PhysicalProperties DeriveOutputProp::visit(SortingStep & step)
{
    const auto & sort_description = step.getSortDescription();
    PhysicalProperties properties{.distribution = {.type = children_prop[0].distribution.type}, .sort_description = sort_description};
    return properties;
}

PhysicalProperties DeriveOutputProp::visit(ExchangeDataStep & step)
{
    return {.distribution = {.type = step.getDistributionType()}, .sort_description = step.getSortDescription()};
}

}
