#include <QueryCoordination/Optimizer/DeriveRequiredChildProp.h>


namespace DB
{

AlternativeChildrenProp DeriveRequiredChildProp::visit(QueryPlanStepPtr step)
{
    if (group_node->hasRequiredChildrenProp())
    {
        return group_node->getRequiredChildrenProp();
    }

    return Base::visit(step);
}

AlternativeChildrenProp DeriveRequiredChildProp::visitDefault(IQueryPlanStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    for (size_t i = 0; i < group_node->childSize(); ++i)
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ReadFromMergeTree & /*step*/)
{
    AlternativeChildrenProp res;
    res.emplace_back();
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(AggregatingStep & step)
{
    AlternativeChildrenProp res;

    if (!step.isPreliminaryAgg())
    {
        std::vector<PhysicalProperties> required_singleton_prop;
        required_singleton_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
        res.emplace_back(required_singleton_prop);

        std::vector<PhysicalProperties> required_hashed_prop;
        PhysicalProperties hashed_prop;
        hashed_prop.distribution.type = PhysicalProperties::DistributionType::Hashed;
        hashed_prop.distribution.keys = step.getParams().keys;
        required_hashed_prop.push_back(hashed_prop);
        res.emplace_back(required_hashed_prop);
    }
    else
    {
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
    }
    return res;
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
        PhysicalProperties hashed_prop;
        hashed_prop.distribution.type = PhysicalProperties::DistributionType::Hashed;
        hashed_prop.distribution.distribution_by_buket_num = true;
        required_child_prop.push_back(hashed_prop);
    }

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ExpressionStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(TopNStep & step)
{
    std::vector<PhysicalProperties> required_child_prop;
    if (step.getPhase() == TopNStep::Phase::Preliminary)
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(SortingStep & step)
{
    std::vector<PhysicalProperties> required_child_prop;
    if (step.getPhase() == SortingStep::Phase::Preliminary)
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(LimitStep & step)
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;

    if (step.getPhase() == LimitStep::Phase::Preliminary)
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

AlternativeChildrenProp DeriveRequiredChildProp::visit(JoinStep & step)
{
    AlternativeChildrenProp res;

    /// broadcast join
    std::vector<PhysicalProperties> broadcast_join_properties;
    broadcast_join_properties.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    broadcast_join_properties.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});
    res.emplace_back(broadcast_join_properties);

    /// shaffle join
    JoinPtr join = step.getJoin();
    const TableJoin & table_join = join->getTableJoin();
    if (table_join.getClauses().size() == 1 && table_join.strictness() != JoinStrictness::Asof) /// broadcast join. Asof support != condition
    {
        auto join_clause = table_join.getOnlyClause(); /// must be equals condition
        std::vector<PhysicalProperties> shaffle_join_prop;
        shaffle_join_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed, .keys = join_clause.key_names_left}});
        shaffle_join_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed, .keys = join_clause.key_names_right}});
        res.emplace_back(shaffle_join_prop);
    }

    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ExchangeDataStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(CreatingSetStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});
    return {required_child_prop};
}

}
