#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ITransformingStep::ITransformingStep(DataStream input_stream, Block output_header, DataStreamTraits traits)
{
    output_stream = DataStream{.header = std::move(output_header)};

    if (traits.preserves_distinct_columns)
    {
        output_stream->distinct_columns = input_streams.front().distinct_columns;
    }

    output_stream->has_single_port = traits.returns_single_stream
                                     || (input_stream.has_single_port && traits.preserves_number_of_streams);

    input_streams.emplace_back(std::move(input_stream));
}

QueryPipelinePtr ITransformingStep::updatePipeline(QueryPipelines pipelines)
{
    transformPipeline(*pipelines.front());
    return std::move(pipelines.front());
}

void ITransformingStep::updateDistinctColumns(const Block & res_header, NameSet & distinct_columns)
{
    if (distinct_columns.empty())
        return;

    for (const auto & column : res_header)
    {
        if (distinct_columns.count(column.name) == 0)
        {
            distinct_columns.clear();
            break;
        }
    }
}

}
