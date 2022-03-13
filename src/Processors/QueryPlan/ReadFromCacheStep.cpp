#include <Processors/QueryPlan/ReadFromCacheStep.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
        {
            {
                .preserves_distinct_columns = true,
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = true,
            }
        };
}


ReadFromCacheStep::ReadFromCacheStep(
    const DB::DataStream & input_stream_, std::unordered_map<IAST::Hash, Data, ASTHash> & cached_results, DB::ASTPtr query_ptr_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , cache(cached_results)
    , query_ptr(query_ptr_)
{}

void ReadFromCacheStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header,  QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ReadFromCacheTransform>(header, cache, query_ptr);
    });
}

}
