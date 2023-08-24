#include <QueryCoordination/Optimizer/DeriveRequiredChildProp.h>


namespace DB
{

AlternativeChildrenProp DeriveRequiredChildProp::visit(QueryPlanStepPtr step)
{
    if (group_node.hasRequiredChildrenProp())
    {
        return group_node.getRequiredChildrenProp();
    }

    return Base::visit(step);
}

AlternativeChildrenProp DeriveRequiredChildProp::visitDefault()
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ReadFromMergeTree & /*step*/)
{
    //    TODO sort_description by pk, DistributionType by distributed table
    AlternativeChildrenProp res;
    res.emplace_back();
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(AggregatingStep & step)
{
    AlternativeChildrenProp res;

    if (step.isFinal())
    {
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

        res.emplace_back(required_child_prop);

        std::vector<PhysicalProperties> required_child_prop1;
        required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

        res.emplace_back(required_child_prop1);

        return res;
    }
    else
    {
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
        return res;
    }
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(MergingAggregatedStep & step)
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;

    if (step.getParams().keys.empty() || !step.isFinal())
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }
    else
    {
        /// TODO Hashed by bucket
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});
    }

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ExpressionStep & /*step*/)
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(SortingStep & /*step*/)
{
    AlternativeChildrenProp res;
    //    const auto & sort_description = step.getSortDescription();
    //    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = sort_description};

    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    res.emplace_back(required_child_prop);
    return res;
}


AlternativeChildrenProp DeriveRequiredChildProp::visit(LimitStep & step)
{
    AlternativeChildrenProp res;

    std::vector<PhysicalProperties> required_child_prop;

    if (step.getType() == LimitStep::Type::Local)
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(JoinStep & /*step*/)
{
    AlternativeChildrenProp res;

    /// broadcast join

    AlternativeChildrenProp alternative_properties;

    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});

    res.emplace_back(required_child_prop);

    /// shaffle join
    AlternativeChildrenProp alternative_properties1;
    std::vector<PhysicalProperties> required_child_prop1;
    required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});
    required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

    res.emplace_back(required_child_prop1);

    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ExchangeDataStep & step)
{
    AlternativeChildrenProp res;

    std::vector<PhysicalProperties> required_child_prop;
    if (step.hasSortInfo())
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = step.getSortDescription()});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }

    res.emplace_back(required_child_prop);
    return res;
}

}
