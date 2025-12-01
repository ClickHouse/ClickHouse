#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include <Common/typeid_cast.h>
#include "Processors/QueryPlan/ReadFromCommonBufferStep.h"
#include "Processors/QueryPlan/SaveSubqueryResultToBufferStep.h"
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

void useMemoryBufferForCommonSubplanResults(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (!subplan_reference)
        return;

    auto * subplan_reference_root = subplan_reference->getSubplanReferenceRoot();
    auto * common_subplan = typeid_cast<CommonSubplanStep *>(subplan_reference_root->step.get());
    if (!common_subplan)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected CommonSubplanReferenceStep to reference CommonSubplanStep, but got {}",
            node.step->getName());

    auto columns_to_use = subplan_reference->extractColumnsToUse();

    auto common_buffer = std::make_shared<ChunkBuffer>();
    node.step = std::make_unique<ReadFromCommonBufferStep>(
        subplan_reference->getOutputHeader(),
        common_buffer,
        settings.max_threads);
    subplan_reference_root->step = std::make_unique<SaveSubqueryResultToBufferStep>(
        subplan_reference_root->step->getOutputHeader(),
        std::move(columns_to_use),
        common_buffer);
}

}

}
