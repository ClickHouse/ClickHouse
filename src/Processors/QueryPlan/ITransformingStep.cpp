#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ITransformingStep::ITransformingStep(DataStream input_stream, Block output_header, Traits traits, bool collect_processors_)
    : transform_traits(std::move(traits.transform_traits))
    , collect_processors(collect_processors_)
    , data_stream_traits(std::move(traits.data_stream_traits))
{
    input_streams.emplace_back(std::move(input_stream));
    output_stream = createOutputStream(input_streams.front(), std::move(output_header), data_stream_traits);
}

/// TODO: cleanup in the following PR.
DataStream ITransformingStep::createOutputStream(
    [[maybe_unused]] const DataStream & input_stream,
    Block output_header,
    [[maybe_unused]] const DataStreamTraits & stream_traits)
{
    DataStream output_stream{.header = std::move(output_header)};

    return output_stream;
}


QueryPipelineBuilderPtr ITransformingStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (collect_processors)
    {
        QueryPipelineProcessorsCollector collector(*pipelines.front(), this);
        transformPipeline(*pipelines.front(), settings);
        processors = collector.detachProcessors();
    }
    else
        transformPipeline(*pipelines.front(), settings);

    return std::move(pipelines.front());
}

void ITransformingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
