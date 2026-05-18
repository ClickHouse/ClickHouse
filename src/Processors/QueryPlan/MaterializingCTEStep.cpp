#include <Planner/Utils.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

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
    MaterializedCTEPtr materialized_cte_
)
    : ITransformingStep(std::move(input_header_), std::make_shared<const Block>(Block{}), getMaterializingCTETraits())
    , materialized_cte(std::move(materialized_cte_))
{
}

void MaterializingCTEStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addMaterializingCTETransform(getOutputHeader(), materialized_cte);
}

void MaterializingCTEStep::describeActions([[maybe_unused]] JSONBuilder::JSONMap & map) const
{
}

void MaterializingCTEStep::describeActions([[maybe_unused]] FormatSettings & settings) const
{
}

MaterializingCTEsStep::MaterializingCTEsStep(SharedHeaders input_headers_)
    : IQueryPlanStep()
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
        /// Multiple `DelayedMaterializingCTEsStep` instances can reference the
        /// same `MaterializedCTE` (e.g. a UNION-level step plus one per UNION
        /// branch, all planted by `addBuildSubqueriesForMaterializedCTEsIfNeeded`).
        /// The first step to reach this CTE in `resolveMaterializingCTEs`'s
        /// pass 1 claims the optimize call; subsequent steps skip. Pattern
        /// parallel to the `is_materialization_planned` claim in
        /// `makePlansForCTEs`.
        if (cte->is_plan_optimized.exchange(true))
            continue;

        /// `cte->plan` can already be `nullptr` if a recursive `buildSetInplace`
        /// path won an earlier `is_materialization_planned` race and
        /// `std::move`d the plan out via `makePlansForCTEs`. Skip the call
        /// rather than throwing an exception; the materialization has
        /// already run inplace.
        if (cte->plan)
            cte->plan->optimize(optimization_settings);
    }
}

std::vector<std::unique_ptr<QueryPlan>> DelayedMaterializingCTEsStep::makePlansForCTEs(DelayedMaterializingCTEsStep && step)
{
    std::vector<std::unique_ptr<QueryPlan>> plans;
    for (auto & materialized_cte : step.ctes)
    {
        if (materialized_cte->is_materialization_planned.exchange(true))
            continue;

        /// The plan was already optimized in the first traversal of
        /// `resolveMaterializingCTEs`. Calling `optimize` again here would
        /// duplicate the recursive `buildSetInplace` work and is no longer
        /// the right place to do it — by the time we reach this point any
        /// safety-net `DelayedMaterializingCTEsStep` inside an IN-subquery
        /// plan that actually needed to claim the CTE has already done so.
        plans.emplace_back(std::move(materialized_cte->plan));
    }
    return plans;
}

void removeTopLevelDelayedMaterializingCTEsStep(QueryPlan & plan)
{
    /// Strip *all* consecutive `DelayedMaterializingCTEsStep` nodes from the
    /// top of `plan`'s materialization chain. The Planner stacks one per
    /// dependency level when `force_materialize_cte` is set
    /// (`addBuildSubqueriesForMaterializedCTEsIfNeeded` pushes one step per
    /// `ctes_by_level` entry on top of the plan), so an IN-subquery that
    /// transitively references a chain of CTEs has multiple safety-net steps
    /// stacked. We must strip them all — leaving any one in place would let
    /// the inner step claim the corresponding CTE in the wrong scope when the
    /// recursive `plan->optimize` below invokes `resolveMaterializingCTEs`.
    ///
    /// When called from `DelayedCreatingSetsStep::makePlansForSets`, the plan
    /// has already been wrapped by `FutureSetFromSubquery::build` with a
    /// `CreatingSetStep` at the root, so the safety-net chain begins at the
    /// root's single child. Otherwise the chain starts at the root itself.
    auto * root = plan.getRootNode();
    if (!root)
        return;

    QueryPlan::Node * parent_of_target = nullptr;
    QueryPlan::Node * target = root;
    if (!typeid_cast<DelayedMaterializingCTEsStep *>(target->step.get()))
    {
        /// Skip past one wrapper step (e.g. `CreatingSetStep` from
        /// `FutureSetFromSubquery::build`) to reach the safety-net chain
        /// below it.
        if (root->children.size() != 1)
            return;
        if (!typeid_cast<DelayedMaterializingCTEsStep *>(root->children.front()->step.get()))
            return;
        parent_of_target = root;
        target = root->children.front();
    }

    while (typeid_cast<DelayedMaterializingCTEsStep *>(target->step.get()))
    {
        if (target->children.size() != 1)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DelayedMaterializingCTEsStep to have exactly one child, got {}",
                target->children.size());

        auto * next = target->children.front();
        if (parent_of_target)
            parent_of_target->children[0] = next;
        else
            plan.replaceRootNode(next);
        target = next;
    }
}

}
