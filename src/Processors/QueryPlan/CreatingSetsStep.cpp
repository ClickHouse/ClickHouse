#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <IO/Operators.h>

namespace DB
{

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
    Block header,
    String description_,
    SubqueryForSet subquery_for_set_,
    SizeLimits network_transfer_limits_,
    const Context & context_)
    : ITransformingStep(input_stream_, header, getTraits())
    , description(std::move(description_))
    , subquery_for_set(std::move(subquery_for_set_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , context(context_)
{
}

void CreatingSetStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.resize(1);
    pipeline.addTransform(
        std::make_shared<CreatingSetsTransform>(
            pipeline.getHeader(),
            getOutputStream().header,
            std::move(subquery_for_set),
            network_transfer_limits,
            context));
}

void CreatingSetStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix;
    if (subquery_for_set.set)
        settings.out << "Set: ";
    else if (subquery_for_set.join)
        settings.out << "Join: ";

    settings.out << description << '\n';
}

CreatingSetsStep::CreatingSetsStep(DataStreams input_streams_)
{
    if (input_streams_.empty())
        throw Exception("CreatingSetsStep cannot be created with no inputs", ErrorCodes::LOGICAL_ERROR);

    input_streams = std::move(input_streams_);
    output_stream = input_streams.front();

    for (size_t i = 1; i < input_streams.size(); ++i)
        assertBlocksHaveEqualStructure(output_stream->header, input_streams[i].header, "CreatingSets");
}

QueryPipelinePtr CreatingSetsStep::updatePipeline(QueryPipelines pipelines)
{
    if (pipelines.empty())
        throw Exception("CreatingSetsStep cannot be created with no inputs", ErrorCodes::LOGICAL_ERROR);

    auto main_pipeline = std::move(pipelines.front());
    if (pipelines.size() == 1)
        return main_pipeline;

    std::swap(pipelines.front(), pipelines.back());
    pipelines.pop_back();

    QueryPipeline delayed_pipeline;
    if (pipelines.size() > 1)
    {
        QueryPipelineProcessorsCollector collector(delayed_pipeline, this);
        delayed_pipeline = QueryPipeline::unitePipelines(std::move(pipelines), output_stream->header);
        processors = collector.detachProcessors();
    }
    else
        delayed_pipeline = std::move(*pipelines.front());

    QueryPipelineProcessorsCollector collector(*main_pipeline, this);
    main_pipeline->addDelayedPipeline(std::move(delayed_pipeline));
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());

    return main_pipeline;
}

void CreatingSetsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
