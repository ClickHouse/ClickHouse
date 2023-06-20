#include <exception>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
//#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/JSONBuilder.h>
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

CreatingSetStep::CreatingSetStep(
    const DataStream & input_stream_,
    String description_,
    SubqueryForSet & subquery_for_set_,
    FutureSetPtr set_,
    SizeLimits network_transfer_limits_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, Block{}, getTraits())
    , description(std::move(description_))
    , subquery_for_set(subquery_for_set_)
    , set(std::move(set_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , context(std::move(context_))
{
}

void CreatingSetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addCreatingSetsTransform(getOutputStream().header, subquery_for_set, std::move(set), network_transfer_limits, context->getPreparedSetsCache());
}

void CreatingSetStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), Block{}, getDataStreamTraits());
}

void CreatingSetStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix;
    if (subquery_for_set.set)
        settings.out << "Set: ";

    settings.out << description << '\n';
}

void CreatingSetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (subquery_for_set.set)
        map.add("Set", description);
}


CreatingSetsStep::CreatingSetsStep(DataStreams input_streams_)
{
    if (input_streams_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CreatingSetsStep cannot be created with no inputs");

    input_streams = std::move(input_streams_);
    output_stream = input_streams.front();

    for (size_t i = 1; i < input_streams.size(); ++i)
        if (input_streams[i].header)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Creating set input must have empty header. Got: {}",
                            input_streams[i].header.dumpStructure());
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

void addCreatingSetsStep(QueryPlan & query_plan, std::vector<std::shared_ptr<FutureSetFromSubquery>> sets_from_subqueries, ContextPtr context)
{
    DataStreams input_streams;
    input_streams.emplace_back(query_plan.getCurrentDataStream());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    query_plan = QueryPlan();

    for (auto & future_set : sets_from_subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(context);
        if (!plan)
            continue;

        input_streams.emplace_back(plan->getCurrentDataStream());
        plans.emplace_back(std::move(plan));
    }

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_streams));
    creating_sets->setStepDescription("Create sets before main query execution");
    query_plan.unitePlans(std::move(creating_sets), std::move(plans));
}

//void addCreatingSetsStep(QueryPlan & query_plan, PreparedSets::SubqueriesForSets subqueries_for_sets, ContextPtr context)

std::vector<std::unique_ptr<QueryPlan>> DelayedCreatingSetsStep::makePlansForSets(DelayedCreatingSetsStep && step)
{
    // DataStreams input_streams;
    // input_streams.emplace_back(query_plan.getCurrentDataStream());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    // plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    // query_plan = QueryPlan();

    for (auto & future_set : step.sets_from_subquery)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(step.context);
        if (!plan)
            continue;

        plan->optimize(QueryPlanOptimizationSettings::fromContext(step.context));

        //input_streams.emplace_back(plan->getCurrentDataStream());
        plans.emplace_back(std::move(plan));
    }

    return plans;
}

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context)
{
    if (!prepared_sets)
        return;

    auto subqueries = prepared_sets->detachSubqueries();
    if (subqueries.empty())
        return;

    addCreatingSetsStep(query_plan, std::move(subqueries), context);
}

DelayedCreatingSetsStep::DelayedCreatingSetsStep(
    DataStream input_stream, std::vector<std::shared_ptr<FutureSetFromSubquery>> sets_from_subquery_, ContextPtr context_)
    : sets_from_subquery(std::move(sets_from_subquery_)), context(std::move(context_))
{
    input_streams = {input_stream};
    output_stream = std::move(input_stream);
}

QueryPipelineBuilderPtr DelayedCreatingSetsStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot build pipeline in DelayedCreatingSets. This step should be optimized out.");
}

}
