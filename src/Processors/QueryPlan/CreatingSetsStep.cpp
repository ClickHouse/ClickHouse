#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/Settings.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
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

CreatingSetStep::CreatingSetStep(
    const Header & input_header_,
    SetAndKeyPtr set_and_key_,
    StoragePtr external_table_,
    SizeLimits network_transfer_limits_,
    PreparedSetsCachePtr prepared_sets_cache_)
    : ITransformingStep(input_header_, Block{}, getTraits())
    , set_and_key(std::move(set_and_key_))
    , external_table(std::move(external_table_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , prepared_sets_cache(std::move(prepared_sets_cache_))
{
}

void CreatingSetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addCreatingSetsTransform(
        getOutputHeader(),
        std::move(set_and_key),
        std::move(external_table),
        network_transfer_limits,
        std::move(prepared_sets_cache));
}

void CreatingSetStep::updateOutputHeader()
{
    output_header = Block{};
}

void CreatingSetStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix;
    if (set_and_key->set)
        settings.out << "Set: ";

    settings.out << set_and_key->key << '\n';
}

void CreatingSetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (set_and_key->set)
        map.add("Set", set_and_key->key);
}


CreatingSetsStep::CreatingSetsStep(Headers input_headers_)
{
    if (input_headers_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CreatingSetsStep cannot be created with no inputs");

    input_headers = std::move(input_headers_);
    output_header = input_headers.front();

    for (size_t i = 1; i < input_headers.size(); ++i)
        if (input_headers[i])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Creating set input must have empty header. Got: {}",
                            input_headers[i].dumpStructure());
}

QueryPipelineBuilderPtr CreatingSetsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CreatingSetsStep cannot be created with no inputs");

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

void CreatingSetsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSets::Subqueries subqueries, ContextPtr context)
{
    Headers input_headers;
    input_headers.emplace_back(query_plan.getCurrentHeader());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    query_plan = QueryPlan();

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context->getPreparedSetsCache();
    for (auto & future_set : subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(network_transfer_limits, prepared_sets_cache);
        if (!plan)
            continue;

        input_headers.emplace_back(plan->getCurrentHeader());
        plans.emplace_back(std::move(plan));
    }

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_headers));
    creating_sets->setStepDescription("Create sets before main query execution");
    query_plan.unitePlans(std::move(creating_sets), std::move(plans));
}

QueryPipelineBuilderPtr addCreatingSetsTransform(QueryPipelineBuilderPtr pipeline, PreparedSets::Subqueries subqueries, ContextPtr context)
{
    Headers input_headers;
    input_headers.emplace_back(pipeline->getHeader());

    QueryPipelineBuilders pipelines;
    pipelines.reserve(1 + subqueries.size());
    pipelines.push_back(std::move(pipeline));

    QueryPlanOptimizationSettings plan_settings(context);
    BuildQueryPipelineSettings pipeline_settings(context);

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context->getPreparedSetsCache();

    for (auto & future_set : subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(network_transfer_limits, prepared_sets_cache);
        if (!plan)
            continue;

        input_headers.emplace_back(plan->getCurrentHeader());
        pipelines.emplace_back(plan->buildQueryPipeline(plan_settings, pipeline_settings));
    }

    return CreatingSetsStep(input_headers).updatePipeline(std::move(pipelines), pipeline_settings);
}

std::vector<std::unique_ptr<QueryPlan>> DelayedCreatingSetsStep::makePlansForSets(
    DelayedCreatingSetsStep && step,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    std::vector<std::unique_ptr<QueryPlan>> plans;

    for (auto & future_set : step.subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(optimization_settings.network_transfer_limits, optimization_settings.prepared_sets_cache);
        if (!plan)
            continue;

        plan->optimize(optimization_settings);

        plans.emplace_back(std::move(plan));
    }

    return plans;
}

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context)
{
    if (!prepared_sets)
        return;

    auto subqueries = prepared_sets->getSubqueries();
    if (subqueries.empty())
        return;

    addCreatingSetsStep(query_plan, std::move(subqueries), context);
}

DelayedCreatingSetsStep::DelayedCreatingSetsStep(
    Header input_header,
    PreparedSets::Subqueries subqueries_,
    SizeLimits network_transfer_limits_,
    PreparedSetsCachePtr prepared_sets_cache_)
    : subqueries(std::move(subqueries_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , prepared_sets_cache(std::move(prepared_sets_cache_))
{
    input_headers = {input_header};
    output_header = std::move(input_header);
}

QueryPipelineBuilderPtr DelayedCreatingSetsStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot build pipeline in DelayedCreatingSets. This step should be optimized out.");
}

}
