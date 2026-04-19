#include <Processors/QueryPlan/MaterializingCTEStep.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 interactive_delay;
}

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

std::vector<std::unique_ptr<QueryPlan>> DelayedMaterializingCTEsStep::makePlansForCTEs(
    DelayedMaterializingCTEsStep && step,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    std::vector<std::unique_ptr<QueryPlan>> plans;

    /// Collected pipeline builders for CTEs that will be materialized inline
    /// below. We execute them all together via `unitePipelines` so CTEs of
    /// the same level materialize in parallel rather than one at a time.
    std::vector<std::unique_ptr<QueryPipelineBuilder>> inline_pipelines;
    std::vector<MaterializedCTEPtr> inline_ctes;

    for (auto & materialized_cte : step.ctes)
    {
        /// Claim this CTE for materialization. Losing an `exchange` race means
        /// another `DelayedMaterializingCTEsStep` (e.g. one wrapping an IN
        /// subquery's source plan, or hoisted above a UNION) has already taken
        /// responsibility; nothing more to do here.
        if (materialized_cte->is_materialization_planned.exchange(true))
            continue;

        materialized_cte->plan->optimize(optimization_settings);

        /// Run the CTE's materialization pipeline synchronously here, inside
        /// the optimization pass. That way any later reader -- an eager
        /// sub-pipeline fired from the same `plan.optimize()` call (via
        /// `KeyCondition -> FutureSetFromSubquery::buildOrderedSetInplace`,
        /// or via `DelayedCreatingSetsStep::makePlansForSets`), a sibling
        /// `DelayedMaterializingCTEsStep` under a UNION, or the main
        /// pipeline itself -- finds the CTE's `StorageMemory` populated
        /// and `is_built=true`. Without this, an eager sub-pipeline can
        /// reach `MemorySource::generate` before the main pipeline has run,
        /// read an empty storage, and throw LOGICAL_ERROR (see #101940 and
        /// #102320). The exactly-once invariant is preserved by the
        /// `exchange` claim above; the returned `plans` list is empty
        /// because the outer tree does not need to run the CTE pipeline.
        ///
        /// This is the same pattern `buildOrderedSetInplace` and
        /// `evaluateScalarSubqueryIfNeeded` use to run sub-pipelines from
        /// an optimization / analyzer pass.
        ///
        /// For EXPLAIN and for deserialized plans without a captured
        /// context we skip the inline execution and wire the CTE's plan
        /// into the outer tree so `EXPLAIN PLAN` stays side-effect-free.
        if (optimization_settings.is_explain || !optimization_settings.query_context)
        {
            plans.emplace_back(std::move(materialized_cte->plan));
            continue;
        }

        auto local_plan = std::move(materialized_cte->plan);
        auto builder = local_plan->buildQueryPipeline(
            optimization_settings,
            BuildQueryPipelineSettings(optimization_settings.query_context));
        inline_pipelines.emplace_back(std::move(builder));
        inline_ctes.emplace_back(materialized_cte);
    }

    /// Execute all inline-materialized CTEs concurrently via a single united
    /// pipeline, so same-level CTEs run in parallel instead of one at a time.
    if (!inline_pipelines.empty())
    {
        QueryPipelineBuilder united = QueryPipelineBuilder::unitePipelines(std::move(inline_pipelines));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(united));
        pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

        CompletedPipelineExecutor executor(pipeline);
        if (optimization_settings.query_context->hasQueryContext())
        {
            if (auto cancel_callback = optimization_settings.query_context->getQueryContext()->getInteractiveCancelCallback())
                executor.setCancelCallback(
                    std::move(cancel_callback),
                    std::max(UInt64(100), optimization_settings.query_context->getSettingsRef()[Setting::interactive_delay] / 1000));
        }
        executor.execute();

        for (const auto & cte : inline_ctes)
        {
            if (!cte->is_built.load(std::memory_order_acquire))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Materialized CTE '{}' pipeline finished but storage was not populated",
                    cte->cte_name);
        }
    }

    return plans;
}

}
