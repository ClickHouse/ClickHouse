#include <Processors/QueryPlan/ConvertingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ConvertingTransform.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true
    };
}

static void filterDistinctColumns(const Block & res_header, NameSet & distinct_columns)
{
    if (distinct_columns.empty())
        return;

    NameSet new_distinct_columns;
    for (const auto & column : res_header)
        if (distinct_columns.count(column.name))
            new_distinct_columns.insert(column.name);

    distinct_columns.swap(new_distinct_columns);
}

ConvertingStep::ConvertingStep(const DataStream & input_stream_, Block result_header_)
        : ITransformingStep(
        input_stream_,
        result_header_,
        getTraits())
        , result_header(std::move(result_header_))
{
    /// Some columns may be removed
    filterDistinctColumns(output_stream->header, output_stream->distinct_columns);
    filterDistinctColumns(output_stream->header, output_stream->local_distinct_columns);
}

void ConvertingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ConvertingTransform>(header, result_header, ConvertingTransform::MatchColumnsMode::Name);
    });
}

}
