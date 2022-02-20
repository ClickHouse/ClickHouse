#include <Processors/QueryPlan/CachingStep.h>
#include <Processors/Transforms/CachingTransform.h>
#include <Parsers/IAST.h>

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

void CachingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header,  QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<CachingTransform>(header, cache, query_ptr);
    });
}

CachingStep::CachingStep(const DataStream & input_stream_, std::unordered_map<IAST::Hash, Data> & cached_results, ASTPtr query_ptr_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , cache(cached_results)
    , query_ptr(query_ptr_)
{}

}
