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


namespace DB
{

//    Distribution distribution;
//
//    SortDescription sort_description;

PhysicalProperties derivationProperties(ReadFromMergeTree & /*step*/)
{
    //    TODO sort_description by pk, DistributionType by distributed table
    return {.distribution = {.type = PhysicalProperties::DistributionType::Random}};
};

PhysicalProperties derivationProperties(AggregatingStep & step)
{
    if (step.isFinal())
    {
        return {.distribution = {.type = PhysicalProperties::DistributionType::Gather}};
    }
    else
    {
        return {.distribution = {.type = PhysicalProperties::DistributionType::Random}};
    }
};

PhysicalProperties derivationProperties(MergingAggregatedStep & step)
{
    return {.distribution = {.type = PhysicalProperties::DistributionType::Hash, .keys = step.getParams().keys}};
};

PhysicalProperties derivationProperties(SortingStep & step)
{

};

PhysicalProperties derivationProperties(JoinStep & step)
{

};

PhysicalProperties derivationProperties(UnionStep & step)
{

};


PhysicalProperties derivationProperties(QueryPlanStepPtr step)
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
}

}
