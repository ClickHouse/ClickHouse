#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPipeline.h>

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

DataStream ITransformingStep::createOutputStream(
    const DataStream & input_stream,
    Block output_header,
    const DataStreamTraits & stream_traits)
{
    DataStream output_stream{.header = std::move(output_header)};

    if (stream_traits.preserves_distinct_columns)
        output_stream.distinct_columns = input_stream.distinct_columns;

    output_stream.has_single_port = stream_traits.returns_single_stream
                                     || (input_stream.has_single_port && stream_traits.preserves_number_of_streams);

    if (stream_traits.preserves_sorting)
    {
        output_stream.sort_description = input_stream.sort_description;
        output_stream.sort_mode = input_stream.sort_mode;
    }

    return output_stream;
}


QueryPipelinePtr ITransformingStep::updatePipeline(QueryPipelines pipelines)
{
    if (collect_processors)
    {
        QueryPipelineProcessorsCollector collector(*pipelines.front(), this);
        transformPipeline(*pipelines.front());
        processors = collector.detachProcessors();
    }
    else
        transformPipeline(*pipelines.front());

    return std::move(pipelines.front());
}

void ITransformingStep::updateDistinctColumns(const Block & res_header, NameSet & distinct_columns)
{
    if (distinct_columns.empty())
        return;

    for (const auto & column : distinct_columns)
    {
        if (!res_header.has(column))
        {
            distinct_columns.clear();
            break;
        }
    }
}

void ITransformingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
