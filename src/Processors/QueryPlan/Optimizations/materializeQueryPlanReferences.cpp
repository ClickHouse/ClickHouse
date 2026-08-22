#include <Common/typeid_cast.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace QueryPlanOptimizations
{

void materializeQueryPlanReferences(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (!subplan_reference)
        return;

    auto columns_to_use = subplan_reference->extractColumnsToUse();

    QueryPlan::cloneSubplanAndReplace(&node, subplan_reference->getSubplanReferenceRoot(), nodes);

    auto * common_subplan = typeid_cast<CommonSubplanStep *>(node.step.get());
    if (!common_subplan)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected CommonSubplanReferenceStep to reference CommonSubplanStep, but got {}",
            node.step->getName());

    if (node.children.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected CommonSubplanStep to have exactly one child, but got {}",
            node.children.size());

    node.step = projectOnlyUsedColumns(common_subplan->getInputHeaders().front(), columns_to_use);
}

void optimizeUnusedCommonSubplans(QueryPlan::Node & node)
{
    auto * common_subplan = typeid_cast<CommonSubplanStep *>(node.step.get());
    if (!common_subplan)
        return;

    if (node.children.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected CommonSubplanStep to have exactly one child, but got {}",
            node.children.size());

    auto * child = node.children[0];

    node.step = std::move(child->step);
    node.children = std::move(child->children);
}

}

}
