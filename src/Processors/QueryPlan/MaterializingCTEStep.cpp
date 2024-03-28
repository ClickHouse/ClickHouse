#include <exception>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include "Processors/Transforms/MaterializingCTETransform.h"
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include "Common/logger_useful.h"
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include "Planner/Utils.h"
#include "Processors/QueryPlan/IQueryPlanStep.h"
#include "QueryPipeline/printPipeline.h"
#include <Processors/QueryPlan/UnionStep.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

MaterializingCTEStep::MaterializingCTEStep(
    const DataStream & input_stream_,
    StoragePtr external_table_,
    String cte_table_name_,
    SizeLimits network_transfer_limits_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, Block{}, getTraits())
    , WithContext(context_)
    , cte_table_name(std::move(cte_table_name_))
    , external_table(std::move(external_table_))
    , network_transfer_limits(std::move(network_transfer_limits_))
{
}

void MaterializingCTEStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // pipeline.addCreatingSetsTransform(getOutputStream().header, std::move(set_and_key), std::move(external_table), network_transfer_limits, context->getPreparedSetsCache());
    pipeline.addMaterializingCTEsTransform(getContext(), getOutputStream().header, external_table, cte_table_name, network_transfer_limits);
}

void MaterializingCTEStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), Block{}, getDataStreamTraits());
}

MaterializingCTEsStep::MaterializingCTEsStep(ContextPtr context_, std::vector<std::unique_ptr<QueryPlan>> && filling_cte_plans_, DataStream input_stream_)
    : WithContext(context_)
    , materializing_cte_plans(std::move(filling_cte_plans_))
{
    input_streams.push_back(std::move(input_stream_));
    for (const auto & plan : materializing_cte_plans)
    {
        auto stream = plan->getCurrentDataStream();
        if (stream.header)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan to fill CTE must not have header.");
    }
    output_stream = DataStream{input_streams.front().header};
}

QueryPipelineBuilderPtr MaterializingCTEsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializingCTEsStep cannot be created with no inputs");

    if (pipelines.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializingCTEsStep cannot be created with multiple inputs");

    auto main_pipeline = std::move(pipelines.front());
    pipelines.erase(pipelines.begin());

    auto context = getContext();
    QueryPipelineBuilders delayed_pipelines;
    for (auto & plan : materializing_cte_plans)
        delayed_pipelines.push_back(plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));

    QueryPipelineBuilder delayed_pipeline;
    if (delayed_pipelines.size() > 1)
        delayed_pipeline = QueryPipelineBuilder::unitePipelines(std::move(delayed_pipelines), context->getSettingsRef().max_threads, &processors);
    else
        delayed_pipeline = std::move(*delayed_pipelines.front());

    QueryPipelineProcessorsCollector main_collector(*main_pipeline, this);
    main_pipeline->addPipelineBefore(std::move(delayed_pipeline));
    auto main_processors = main_collector.detachProcessors();
    processors.insert(processors.end(), main_processors.begin(), main_processors.end());

    return main_pipeline;
}

void MaterializingCTEsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);

    for (const auto & plan : materializing_cte_plans)
        plan->explainPipeline(settings.out, {settings.write_header}, settings.offset);
}

// DelayedMaterializingCTEsStep::DelayedMaterializingCTEsStep(
//     DataStream input_stream, FutureTablesFromCTE && future_tables_, ContextPtr context_)
//     : future_tables(std::move(future_tables_)), context(std::move(context_))
// {
//     input_streams = {input_stream};
//     output_stream = std::move(input_stream);
// }

// QueryPipelineBuilderPtr DelayedMaterializingCTEsStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
// {
//     throw Exception(
//         ErrorCodes::LOGICAL_ERROR,
//         "Cannot build pipeline in DelayedCreatingSets. This step should be optimized out.");
// }

}
