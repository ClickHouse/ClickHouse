#pragma once

#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/ScanStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <QueryCoordination/NewOptimizer/GroupNode.h>


namespace DB
{

//    Distribution distribution;
//
//    SortDescription sort_description;

AlternativeChildrenProp derivationRequiredChildProp(ReadFromMergeTree & /*step*/)
{
    //    TODO sort_description by pk, DistributionType by distributed table
    AlternativeChildrenProp res;
    res.emplace_back();
    return res;
};

AlternativeChildrenProp derivationRequiredChildProp(AggregatingStep & step)
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
};

AlternativeChildrenProp derivationRequiredChildProp(MergingAggregatedStep & step)
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;

    if (step.getParams().keys.empty()/* || step.withTotalsOrCubeOrRollup()*/)
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
};

AlternativeChildrenProp derivationRequiredChildProp(ExpressionStep & /*step*/)
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    res.emplace_back(required_child_prop);
    return res;
};

AlternativeChildrenProp derivationRequiredChildProp(SortingStep & /*step*/)
{
    AlternativeChildrenProp res;
//    const auto & sort_description = step.getSortDescription();
//    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = sort_description};

    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    res.emplace_back(required_child_prop);
    return res;
};


AlternativeChildrenProp derivationRequiredChildProp(LimitStep & step)
{
    AlternativeChildrenProp res;

    std::vector<PhysicalProperties> required_child_prop;

    if (step.getStepDescription().contains("preliminary"))
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }

    res.emplace_back(required_child_prop);
    return res;
};

AlternativeChildrenProp derivationRequiredChildProp(JoinStep & /*step*/)
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
};

AlternativeChildrenProp derivationRequiredChildProp(UnionStep & /*step*/)
{
    AlternativeChildrenProp res;
    return res;
};

AlternativeChildrenProp derivationRequiredChildProp(ExchangeDataStep & step)
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
};

AlternativeChildrenProp derivationRequiredChildProp(GroupNode & group_node)
{
    if (group_node.hasRequiredChildrenProp())
    {
        return group_node.getRequiredChildrenProp();
    }

    if (auto * scan_step = dynamic_cast<ReadFromMergeTree *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*scan_step);
    }
    else if (auto * agg_step = dynamic_cast<AggregatingStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*agg_step);
    }
    else if (auto * merge_agg_step = dynamic_cast<MergingAggregatedStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*merge_agg_step);
    }
    else if (auto * sort_step = dynamic_cast<SortingStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*sort_step);
    }
    else if (auto * join_step = dynamic_cast<JoinStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*join_step);
    }
    else if (auto * union_step = dynamic_cast<UnionStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*union_step);
    }
    else if (auto * exchange_step = dynamic_cast<ExchangeDataStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*exchange_step);
    }
    else if (auto * limit_step = dynamic_cast<LimitStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*limit_step);
    }
    else if (auto * expression_step = dynamic_cast<ExpressionStep *>(group_node.getStep().get()))
    {
        return derivationRequiredChildProp(*expression_step);
    }
    else
    {
        AlternativeChildrenProp res;
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
        return res;
    }
}

}
