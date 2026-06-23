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

void materializeQueryPlanReferences(QueryPlan::Node & node, QueryPlan::Nodes & nodes, bool use_in_memory_buffer)
{
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (!subplan_reference)
        return;

    /// When the in-memory buffer is enabled globally, only references that explicitly require
    /// materialization are materialized here; the rest are handled by useMemoryBufferForCommonSubplanResult.
    /// A reference requires materialization when its decorrelation join puts the subquery input on the
    /// probe side (join_kind = left), where the buffer cannot guarantee producer-before-consumer ordering.
    if (use_in_memory_buffer && !subplan_reference->mustMaterialize())
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

void optimizeUnusedCommonSubplans(QueryPlan::Node & node, const std::unordered_set<const QueryPlan::Node *> & referenced_subplan_roots)
{
    auto * common_subplan = typeid_cast<CommonSubplanStep *>(node.step.get());
    if (!common_subplan)
        return;

    /// A CommonSubplanStep can still be referenced by a CommonSubplanReferenceStep that was left for the
    /// in-memory buffer optimization (e.g. when only some references were materialized). Unwrapping it here
    /// would detach the producer the buffer needs, so keep it; useMemoryBufferForCommonSubplanResult will
    /// replace it with a SaveSubqueryResultToBufferStep.
    if (referenced_subplan_roots.contains(&node))
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
