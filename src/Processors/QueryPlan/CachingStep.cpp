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
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<CachingTransform>(header, cache, query_ptr);
    });
}

CachingStep::CachingStep(const DataStream & input_stream_, LRUCache<CacheKey, Data, CacheKeyHasher> & cache_, ASTPtr query_ptr_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , cache(cache_)
    , query_ptr(query_ptr_)
{}

}
