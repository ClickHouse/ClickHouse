#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/JSONBuilder.h>
#include <Interpreters/PreparedSets.h>

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
            .preserves_distinct_columns = true,
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
    SubqueryForSet subquery_for_set_,
    SizeLimits network_transfer_limits_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, Block{}, getTraits())
    , WithContext(context_)
    , description(std::move(description_))
    , subquery_for_set(std::move(subquery_for_set_))
    , network_transfer_limits(std::move(network_transfer_limits_))
{
}

void CreatingSetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addCreatingSetsTransform(getOutputStream().header, std::move(subquery_for_set), network_transfer_limits, getContext());
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
        throw Exception("CreatingSetsStep cannot be created with no inputs", ErrorCodes::LOGICAL_ERROR);

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
        throw Exception("CreatingSetsStep cannot be created with no inputs", ErrorCodes::LOGICAL_ERROR);

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

void addCreatingSetsStep(
    QueryPlan & query_plan, PreparedSets & prepared_sets, const SizeLimits & limits, ContextPtr context)
{
    DataStreams input_streams;
    input_streams.emplace_back(query_plan.getCurrentDataStream());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    query_plan = QueryPlan();

    for (auto & [description, subquery_for_set] : prepared_sets.moveSubqueries())
    {
        if (!subquery_for_set.hasSource())
            continue;

        auto plan = subquery_for_set.moveSource();

        auto creating_set = std::make_unique<CreatingSetStep>(
                plan->getCurrentDataStream(),
                description,
                std::move(subquery_for_set),
                limits,
                context);
        creating_set->setStepDescription("Create set for subquery");
        plan->addStep(std::move(creating_set));

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

}
