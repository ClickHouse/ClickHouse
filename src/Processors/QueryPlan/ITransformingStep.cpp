#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ITransformingStep::ITransformingStep(DataStream input_stream, Block output_header, DataStreamTraits traits)
{
    input_streams.emplace_back(std::move(input_stream));
    output_stream = DataStream{.header = std::move(output_header)};

    if (traits.preserves_distinct_columns)
    {
        output_stream->distinct_columns = input_streams.front().distinct_columns;
        output_stream->local_distinct_columns = input_streams.front().local_distinct_columns;
    }
}

QueryPipelinePtr ITransformingStep::updatePipeline(QueryPipelines pipelines)
{
    transformPipeline(*pipelines.front());
    return std::move(pipelines.front());
}

}
