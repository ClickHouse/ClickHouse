#include <exception>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Processors/Transforms/MaterializingCTETransform.h>
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
    FutureTableFromCTEPtr future_table_,
    SizeLimits network_transfer_limits_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, Block{}, getTraits())
    , WithContext(context_)
    , future_table(std::move(future_table_))
    , network_transfer_limits(std::move(network_transfer_limits_))
{
}

void MaterializingCTEStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addMaterializingCTEsTransform(getContext(), getOutputStream().header, future_table, network_transfer_limits);
}

void MaterializingCTEStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), Block{}, getDataStreamTraits());
}

MaterializingCTEsStep::MaterializingCTEsStep(ContextPtr context_, FutureTablesFromCTE && future_tables_, DataStream input_stream_)
    : WithContext(context_)
    , future_tables(std::move(future_tables_))
{
    input_streams.push_back(std::move(input_stream_));
    output_stream = DataStream{input_streams.front().header};
}

QueryPipelineBuilderPtr MaterializingCTEsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializingCTEsStep can only be created with a single input");

    auto main_pipeline = std::move(pipelines.front());
    pipelines.clear();

    auto context = getContext();
    QueryPipelineBuilders delayed_pipelines;

    for (auto & future_table : future_tables)
    {
        auto [plan, _] = future_table->buildPlanOrGetPromiseToMaterialize(context);
        /// It means that the table is already materialized in previous optimization steps for index analysis
        /// We don't need to wait here
        if (plan)
        {
            delayed_pipelines.push_back(plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));
            materializing_future_table_plans.push_back(std::move(plan));
        }
    }

    if (!delayed_pipelines.empty())
    {
        QueryPipelineBuilder delayed_pipeline;
        if (delayed_pipelines.size() == 1)
            delayed_pipeline = QueryPipelineBuilder::unitePipelines(std::move(delayed_pipelines), context->getSettingsRef().max_threads, &processors);
        else
            delayed_pipeline = std::move(*delayed_pipelines.front());

        QueryPipelineProcessorsCollector main_collector(*main_pipeline, this);
        main_pipeline->addPipelineBefore(std::move(delayed_pipeline));
        auto main_processors = main_collector.detachProcessors();
        processors.insert(processors.end(), main_processors.begin(), main_processors.end());
    }

    return main_pipeline;
}

void MaterializingCTEsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);

    for (const auto & plan : materializing_future_table_plans)
        plan->explainPipeline(settings.out, {settings.write_header}, settings.offset);
}

}
