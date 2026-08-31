#include <Common/typeid_cast.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
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

namespace
{

/// `DelayedCreatingSetsStep` is a transparent placeholder (its output header equals its input
/// header), so after its sets are taken the node is spliced out of the tree.
void takeSetsAndSpliceOut(QueryPlan::Node *& slot, PreparedSets::Subqueries & sets)
{
    while (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(slot->step.get()))
    {
        auto step_sets = delayed->detachSets();
        std::move(step_sets.begin(), step_sets.end(), std::back_inserter(sets));
        slot = slot->children.front();
    }
}

PreparedSets::Subqueries extractSetsForMaterialization(QueryPlan::Node & subplan_root)
{
    PreparedSets::Subqueries sets;

    /// The root of the referenced subplan cannot be removed from here (the reference points at it),
    /// so only its sets are taken out; an emptied `DelayedCreatingSetsStep` builds nothing and its
    /// header is unchanged.
    if (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(subplan_root.step.get()))
    {
        auto step_sets = delayed->detachSets();
        std::move(step_sets.begin(), step_sets.end(), std::back_inserter(sets));
    }

    std::vector<QueryPlan::Node *> stack{&subplan_root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        for (auto & child : node->children)
        {
            takeSetsAndSpliceOut(child, sets);
            stack.push_back(child);
        }

        /// Some steps own whole subplans (e.g. `ReadFromMerge` children), whose root node cannot be
        /// removed from here either.
        for (auto * child_plan : node->step->getChildPlans())
        {
            auto * child_root = child_plan ? child_plan->getRootNode() : nullptr;
            if (!child_root)
                continue;
            if (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(child_root->step.get()))
            {
                auto step_sets = delayed->detachSets();
                std::move(step_sets.begin(), step_sets.end(), std::back_inserter(sets));
            }
            stack.push_back(child_root);
        }
    }

    return sets;
}

}

void materializeQueryPlanReferences(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, std::vector<FutureSetFromSubqueryPtr> & extracted_sets)
{
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (!subplan_reference)
        return;

    auto columns_to_use = subplan_reference->extractColumnsToUse();

    /// A `FutureSetFromSubquery` owns its source plan once: `FutureSetFromSubquery::build` moves the
    /// source out, and every later claimant sees no plan and builds nothing. Cloning a fragment that
    /// contains the step holding such a set would create a second claimant, so the sets are taken out
    /// of the fragment first and one builder that dominates every copy is attached by the caller.
    auto subplan_sets = extractSetsForMaterialization(*subplan_reference->getSubplanReferenceRoot());
    std::move(subplan_sets.begin(), subplan_sets.end(), std::back_inserter(extracted_sets));

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
