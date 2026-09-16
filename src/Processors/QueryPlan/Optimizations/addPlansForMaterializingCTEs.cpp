#include <Common/typeid_cast.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

void addPlansForMaterializingCTEs(
    QueryPlan & root_plan,
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes)
{
    auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(node.step.get());
    if (!delayed)
        return;

    auto plans = DelayedMaterializingCTEsStep::makePlansForCTEs(std::move(*delayed));

    SharedHeaders input_headers;
    input_headers.reserve(1 + plans.size());
    input_headers.push_back(node.children.front()->step->getOutputHeader());

    for (auto & plan : plans)
    {
        input_headers.push_back(plan->getCurrentHeader());
        node.children.push_back(plan->getRootNode());
        auto [add_nodes, add_resources] = QueryPlan::detachNodesAndResources(std::move(*plan));
        nodes.splice(nodes.end(), std::move(add_nodes));
        root_plan.addResources(std::move(add_resources));
    }

    auto materializing_ctes = std::make_unique<MaterializingCTEsStep>(std::move(input_headers));
    materializing_ctes->setStepDescription("Materialize CTEs before main query execution");
    node.step = std::move(materializing_ctes);
}

/// Walk the plan tree and ask each `DelayedMaterializingCTEsStep` to optimize
/// its CTEs' subplans.
///
/// Returns `true` iff at least one `DelayedMaterializingCTEsStep` was found in
/// the tree. This is the only place where `DelayedMaterializingCTEsStep::optimizePlans`
/// is invoked, which in turn calls `materialized_cte->plan->optimize(...)` for
/// each CTE the step owns. The recursive optimize call drives — via
/// `addStepsToBuildSets` on the CTE's plan and via
/// `KeyCondition::tryPrepareSetIndexForIn` / `VirtualColumnUtils::buildSetsForDAG`
/// on any MergeTree read inside the CTE — the synchronous
/// `FutureSetFromSubquery::buildSetInplace` path. When that path fires, the
/// safety-net `DelayedMaterializingCTEsStep` planted inside the IN-subquery plan
/// by `Planner.cpp`'s `forceMaterializeCTE` is the first to call
/// `makePlansForCTEs` for the CTE, so it wins the
/// `is_materialization_planned.exchange(true)` race and materializes the CTE
/// inplace before this function returns.
bool optimizeMaterializedCTEPlans(
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan::Node & root)
{
    bool found_delayed = false;
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(frame.node->step.get()))
        {
            found_delayed = true;
            delayed->optimizePlans(optimization_settings);
        }

        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }

    return found_delayed;
}

}

void resolveMaterializingCTEs(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & root_plan, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    /// First traversal: optimize the pre-built plan of every materialized CTE
    /// referenced by a `DelayedMaterializingCTEsStep` in the tree. The
    /// optimize call may chain into `buildSetInplace`, which in turn invokes
    /// `resolveMaterializingCTEs` on an IN-subquery's plan; that recursive
    /// call gets to claim the CTE before we attempt the outer claim below.
    ///
    /// If no DelayedMaterializingCTEsStep exists in the tree at all (the
    /// common case for queries that do not use materialized CTEs) the second
    /// traversal would be a guaranteed no-op, so we skip it. Traversal 1 does
    /// not mutate the main plan tree — only the CTEs' own plans — so the set
    /// of DelayedMaterializingCTEsStep nodes that traversal 2 would visit is
    /// exactly the set traversal 1 already saw.
    if (!optimizeMaterializedCTEPlans(optimization_settings, root))
        return;

    /// Second traversal: replace every remaining `DelayedMaterializingCTEsStep`
    /// with a `MaterializingCTEsStep`, attaching the plans of CTEs that have
    /// not yet been claimed inplace.
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        addPlansForMaterializingCTEs(root_plan, *frame.node, nodes);

        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }
}

}
