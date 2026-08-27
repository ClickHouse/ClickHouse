#include <Planner/Utils.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <functional>
#include <stack>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace
{

constexpr ITransformingStep::Traits getMaterializingCTETraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

}

MaterializingCTEStep::MaterializingCTEStep(
    SharedHeader input_header_,
    MaterializedCTEWeakPtr materialized_cte_
)
    : ITransformingStep(std::move(input_header_), std::make_shared<const Block>(Block{}), getMaterializingCTETraits())
    , materialized_cte(std::move(materialized_cte_))
{
}

void MaterializingCTEStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto cte = materialized_cte.lock();
    if (!cte)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized CTE was destroyed before a pipeline was built for its plan");

    pipeline.addMaterializingCTETransform(getOutputHeader(), std::move(cte));
}

void MaterializingCTEStep::describeActions([[maybe_unused]] JSONBuilder::JSONMap & map) const
{
}

void MaterializingCTEStep::describeActions([[maybe_unused]] FormatSettings & settings) const
{
}

MaterializingCTEsStep::MaterializingCTEsStep(SharedHeaders input_headers_, std::vector<MaterializedCTEPtr> ctes_)
    : ctes(std::move(ctes_))
{
    input_headers = std::move(input_headers_);
    output_header = input_headers.front();
}

QueryPipelineBuilderPtr MaterializingCTEsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializingCTEsStep cannot be created with no inputs");

    auto main_pipeline = std::move(pipelines.front());
    if (pipelines.size() == 1)
        return main_pipeline;

    pipelines.erase(pipelines.begin());

    QueryPipelineBuilder delayed_pipeline;
    if (pipelines.size() > 1)
    {
        QueryPipelineProcessorsCollector collector(delayed_pipeline, this);
        delayed_pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines));
        processors = collector.detachProcessors();
    }
    else
        delayed_pipeline = std::move(*pipelines.front());

    QueryPipelineProcessorsCollector collector(*main_pipeline, this);
    main_pipeline->addPipelineBefore(std::move(delayed_pipeline));
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());

    return main_pipeline;
}

DelayedMaterializingCTEsStep::DelayedMaterializingCTEsStep(
    SharedHeader input_header,
    std::vector<MaterializedCTEPtr> ctes_
)
    : ctes(std::move(ctes_))
{
    input_headers = {input_header};
    output_header = std::move(input_header);
}

QueryPipelineBuilderPtr DelayedMaterializingCTEsStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot build pipeline in DelayedMaterializingCTEs. This step should be optimized out.");
}

void DelayedMaterializingCTEsStep::optimizePlans(const QueryPlanOptimizationSettings & optimization_settings)
{
    for (const auto & cte : ctes)
    {
        /// The plan is absent when `makePlansForCTEs` already moved it out, or when the
        /// reference reached this list before its plan was built. Skipping before the
        /// claim leaves the flag for whoever holds the plan, so that plan is optimized.
        if (!cte->plan)
            continue;

        /// Multiple `DelayedMaterializingCTEsStep` instances can reference the
        /// same `MaterializedCTE` (e.g. a UNION-level step plus one per UNION
        /// branch, all planted by `addBuildSubqueriesForMaterializedCTEsIfNeeded`).
        /// The first step to reach this CTE in `resolveMaterializingCTEs`'s
        /// pass 1 claims the optimize call; subsequent steps skip. Pattern
        /// parallel to the `is_materialization_planned` claim in
        /// `makePlansForCTEs`.
        if (cte->is_plan_optimized.exchange(true))
            continue;

        cte->plan->optimize(optimization_settings);
    }
}

bool DelayedMaterializingCTEsStep::eraseCTEs(const MaterializedCTESet & ctes_to_erase)
{
    std::erase_if(ctes, [&](const MaterializedCTEPtr & cte) { return ctes_to_erase.contains(cte); });
    return ctes.empty();
}

std::vector<DelayedMaterializingCTEsStep::ClaimedCTE> DelayedMaterializingCTEsStep::makePlansForCTEs(DelayedMaterializingCTEsStep && step)
{
    std::vector<ClaimedCTE> claimed;
    for (auto & materialized_cte : step.ctes)
    {
        /// Every claimed entry carries a plan, so the attach site can dereference it. Skipping
        /// before the claim leaves the flag for whoever holds the subquery that will plan it.
        if (!materialized_cte->plan)
            continue;

        if (materialized_cte->is_materialization_planned.exchange(true))
            continue;

        /// The plan was already optimized in the first traversal of
        /// `resolveMaterializingCTEs`. Calling `optimize` again here would
        /// duplicate the recursive `buildSetInplace` work and is no longer
        /// the right place to do it — by the time we reach this point any
        /// safety-net `DelayedMaterializingCTEsStep` inside an IN-subquery
        /// plan that actually needed to claim the CTE has already done so.
        ///
        /// The CTE handle travels with its plan so that the step which
        /// attaches the plan also becomes the CTE's owner: the plan holds
        /// only a weak back-reference (`MaterializingCTEStep`), and CTEs
        /// claimed elsewhere must stay out of this list.
        claimed.push_back({materialized_cte, std::move(materialized_cte->plan)});
    }
    return claimed;
}

/// Strip *every* `DelayedMaterializingCTEsStep` node from `plan`'s tree,
/// wherever it sits — at the root, under a single wrapper step (e.g.
/// `CreatingSetStep` planted by `FutureSetFromSubquery::build`), or
/// arbitrarily deep, including the per-branch safety-nets that
/// `buildPlanForQueryNode` plants below each branch of a `UnionStep` /
/// `IntersectOrExceptStep` in addition to the union-level one planted by
/// `buildPlanForUnionNode`.
///
/// Called from `DelayedCreatingSetsStep::makePlansForSets` when attaching a
/// pre-built IN-subquery plan for *runtime* set construction. The strip is
/// what guarantees the outer query plan's `resolveMaterializingCTEs` wins
/// `is_materialization_planned.exchange(true)` for every referenced CTE — if
/// any `DelayedMaterializingCTEsStep` survives in the sub-plan tree, the
/// sub-plan's recursive `plan->optimize` will claim the CTE first, attach the
/// writer somewhere inside the sub-plan tree (in the worst case inside a
/// single `UnionStep` branch), and leave the outer-plan step degenerate. That
/// loses the lazy back-pressure path through the outer `MaterializingCTEsStep`'s
/// `DelayedPortsProcessor` that gates every reader in the sub-plans
/// (including readers on the "main" side of inner `CreatingSets` gates,
/// which are only `setNeeded` after the outer gate first wakes them - see
/// `DelayedPortsProcessor::prepare` lines 130-138).
///
/// Nested `DelayedCreatingSetsStep` source plans are *not* touched: their
/// plans live in `subqueries`, not as children of any node in the immediate
/// tree, so a tree walk of the immediate plan does not reach them. Their
/// own safety-nets remain available for their own `buildSetInplace` /
/// `buildOrderedSetInplace` consumers.
///
/// `predicate` is invoked once per `DelayedMaterializingCTEsStep` node
/// found; the node is spliced out of the tree only when it returns true. It is
/// allowed to mutate the step (that is how `removeDelayedMaterializingCTEsStepFor`
/// drops a subset of a step's CTEs and keeps the rest).
static void removeDelayedMaterializingCTEsStepIf(
    QueryPlan & plan,
    const std::function<bool(DelayedMaterializingCTEsStep &)> & predicate)
{
    /// Strip any `DelayedMaterializingCTEsStep` chain at the root via
    /// `replaceRootNode`. We loop because consecutive root-level safety-nets
    /// can be stacked (one per dependency level when
    /// `addBuildSubqueriesForMaterializedCTEsIfNeeded` pushes one step per
    /// `ctes_by_level` entry).
    while (true)
    {
        auto * root = plan.getRootNode();
        if (!root)
            return;
        auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(root->step.get());
        if (!delayed || !predicate(*delayed))
            break;
        if (root->children.size() != 1)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DelayedMaterializingCTEsStep to have exactly one child, got {}",
                root->children.size());
        plan.replaceRootNode(root->children.front());
    }

    auto * root = plan.getRootNode();
    if (!root)
        return;

    /// Walk every node below the root; for each child pointer, while the
    /// referenced child is a `DelayedMaterializingCTEsStep` that must go,
    /// replace the pointer with its single grandchild. The orphaned step's
    /// `Node` remains in `QueryPlan::nodes` (memory is owned by the list) but
    /// is no longer reachable from the root.
    std::stack<QueryPlan::Node *> stack;
    stack.push(root);
    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        for (auto & child : node->children)
        {
            while (auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(child->step.get()))
            {
                if (!predicate(*delayed))
                    break;
                if (child->children.size() != 1)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Expected DelayedMaterializingCTEsStep to have exactly one child, got {}",
                        child->children.size());
                child = child->children.front();
            }
        }

        for (auto * child : node->children)
            stack.push(child);
    }
}

void removeAllDelayedMaterializingCTEsStep(QueryPlan & plan)
{
    removeDelayedMaterializingCTEsStepIf(plan, [](DelayedMaterializingCTEsStep &) { return true; });
}

void removeDelayedMaterializingCTEsStepFor(QueryPlan & plan, const MaterializedCTESet & ctes_to_remove)
{
    if (ctes_to_remove.empty())
        return;

    removeDelayedMaterializingCTEsStepIf(
        plan, [&](DelayedMaterializingCTEsStep & step) { return step.eraseCTEs(ctes_to_remove); });
}

}
