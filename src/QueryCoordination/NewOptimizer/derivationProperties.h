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

PropAndAlternativeChildrenProp derivationProperties(ReadFromMergeTree & /*step*/)
{
    //    TODO sort_description by pk, DistributionType by distributed table
    PropAndAlternativeChildrenProp res;
    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};
    res[properties];
    return res;
};

PropAndAlternativeChildrenProp derivationProperties(AggregatingStep & step)
{
    PropAndAlternativeChildrenProp res;

    if (step.isFinal())
    {
        PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}};

        AlternativeChildrenProp alternative_properties;

        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

        alternative_properties.emplace_back(required_child_prop);

        res[properties] = alternative_properties;

        PhysicalProperties properties1{.distribution = {.type = PhysicalProperties::DistributionType::Hashed}};

        AlternativeChildrenProp alternative_properties1;
        std::vector<PhysicalProperties> required_child_prop1;
        required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

        alternative_properties1.emplace_back(required_child_prop1);

        res[properties1] = alternative_properties1;

        return res;
    }
    else
    {
        PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};

        AlternativeChildrenProp alternative_properties;
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});

        alternative_properties.emplace_back(required_child_prop);
        res[properties] = alternative_properties;

        return res;
    }
};

PropAndAlternativeChildrenProp derivationProperties(MergingAggregatedStep & /*step*/)
{
    PropAndAlternativeChildrenProp res;
    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Hashed}};

    AlternativeChildrenProp alternative_properties;
    std::vector<PhysicalProperties> required_child_prop;

    /// TODO Hashed by bucket
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

    alternative_properties.emplace_back(required_child_prop);
    res[properties] = alternative_properties;

    return res;
};

PropAndAlternativeChildrenProp derivationProperties(ExpressionStep & /*step*/)
{
    PropAndAlternativeChildrenProp res;

    AlternativeChildrenProp alternative_properties;
    std::vector<PhysicalProperties> required_child_prop;

    PhysicalProperties properties;

    properties.distribution = {.type = PhysicalProperties::DistributionType::Any};
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});

    alternative_properties.emplace_back(required_child_prop);
    res[properties] = alternative_properties;
    return res;
};

PropAndAlternativeChildrenProp derivationProperties(SortingStep & step)
{
    PropAndAlternativeChildrenProp res;
    const auto & sort_description = step.getSortDescription();
    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = sort_description};

    AlternativeChildrenProp alternative_properties;
    std::vector<PhysicalProperties> required_child_prop;

    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});

    alternative_properties.emplace_back(required_child_prop);
    res[properties] = alternative_properties;

    return res;
};


PropAndAlternativeChildrenProp derivationProperties(LimitStep & step)
{
    PropAndAlternativeChildrenProp res;

    PhysicalProperties properties;
    AlternativeChildrenProp alternative_properties;
    std::vector<PhysicalProperties> required_child_prop;

    if (step.getStepDescription().contains("preliminary"))
    {
        properties.distribution = {.type = PhysicalProperties::DistributionType::Any};
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }
    else
    {
        properties.distribution = {.type = PhysicalProperties::DistributionType::Singleton};
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }

    alternative_properties.emplace_back(required_child_prop);
    res[properties] = alternative_properties;
    return res;
};

PropAndAlternativeChildrenProp derivationProperties(JoinStep & /*step*/)
{
    PropAndAlternativeChildrenProp res;

    /// broadcast join
    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};

    AlternativeChildrenProp alternative_properties;

    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});

    alternative_properties.emplace_back(required_child_prop);

    res[properties] = alternative_properties;

    /// shuffle join or co-localed join
    PhysicalProperties properties1{.distribution = {.type = PhysicalProperties::DistributionType::Hashed}};

    AlternativeChildrenProp alternative_properties1;
    std::vector<PhysicalProperties> required_child_prop1;
    required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});
    required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

    alternative_properties1.emplace_back(required_child_prop1);

    res[properties1] = alternative_properties1;

    return res;
};

PropAndAlternativeChildrenProp derivationProperties(UnionStep & /*step*/)
{
    PropAndAlternativeChildrenProp res;
    return res;
};

PropAndAlternativeChildrenProp derivationProperties(ExchangeDataStep & step)
{
    PropAndAlternativeChildrenProp res;
    PhysicalProperties properties{.distribution = {.type = step.getDistributionType()}};

    AlternativeChildrenProp alternative_properties;
    std::vector<PhysicalProperties> required_child_prop;
    if (step.hasSortInfo())
    {
        properties.sort_description = step.getSortDescription();
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = step.getSortDescription()});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }

    alternative_properties.emplace_back(required_child_prop);
    res[properties] = alternative_properties;
    return res;
};

PropAndAlternativeChildrenProp derivationProperties(QueryPlanStepPtr step)
{
    if (auto * scan_step = dynamic_cast<ReadFromMergeTree *>(step.get()))
    {
        return derivationProperties(*scan_step);
    }
    else if (auto * agg_step = dynamic_cast<AggregatingStep *>(step.get()))
    {
        return derivationProperties(*agg_step);
    }
    else if (auto * merge_agg_step = dynamic_cast<MergingAggregatedStep *>(step.get()))
    {
        return derivationProperties(*merge_agg_step);
    }
    else if (auto * sort_step = dynamic_cast<SortingStep *>(step.get()))
    {
        return derivationProperties(*sort_step);
    }
    else if (auto * join_step = dynamic_cast<JoinStep *>(step.get()))
    {
        return derivationProperties(*join_step);
    }
    else if (auto * union_step = dynamic_cast<UnionStep *>(step.get()))
    {
        return derivationProperties(*union_step);
    }
    else if (auto * exchange_step = dynamic_cast<ExchangeDataStep *>(step.get()))
    {
        return derivationProperties(*exchange_step);
    }
    else if (auto * limit_step = dynamic_cast<LimitStep *>(step.get()))
    {
        return derivationProperties(*limit_step);
    }
    else if (auto * expression_step = dynamic_cast<ExpressionStep *>(step.get()))
    {
        return derivationProperties(*expression_step);
    }
//    else if (dynamic_cast<CreatingSetStep *>(root_node.step.get()))
//    {
//        /// Do noting, add to brother fragment
//        result = child_fragments[0];
//    }
//    else if (dynamic_cast<CreatingSetsStep *>(root_node.step.get()))
//    {
//        /// CreatingSetsStep need push to child_fragments[0], connect child_fragments[0] to child_fragments[1-N]
//        result = createCreatingSetsFragment(root_node, child_fragments);
//    }
//    else if (needPushDownChild(root_node.step))
//    {
//        /// not Projection ExpressionStep push it to child_fragments[0]
//
//        child_fragments[0]->addStep(root_node.step);
//        result = child_fragments[0];
//    }
//    else if (isLimitRelated(root_node.step))
//    {
//        pushDownLimitRelated(root_node.step, child_fragments[0]);
//        result = child_fragments[0];
//    }
    PropAndAlternativeChildrenProp res;
    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};

    AlternativeChildrenProp alternative_properties;
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});

    alternative_properties.emplace_back(required_child_prop);
    res[properties] = alternative_properties;
    return res;
}

}
