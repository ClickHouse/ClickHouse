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
        /// The same MaterializedCTE pointer can be referenced by both the
        /// outer step (in the main plan) and a safety-net step (inside an
        /// IN-subquery plan). The recursive `buildSetInplace` path might
        /// have already moved a CTE's plan out via `makePlansForCTEs`.
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
    auto * root = plan.getRootNode();
    if (!root)
        return;

    auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(root->step.get());
    if (!delayed)
        return;

    /// `DelayedMaterializingCTEsStep` is a single-input passthrough at the
    /// plan level — it has exactly one child (the wrapped body of the
    /// IN-subquery). Replace the root with that child.
    if (root->children.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected DelayedMaterializingCTEsStep to have exactly one child, got {}",
            root->children.size());

    plan.replaceRootNode(root->children.front());
}

}
