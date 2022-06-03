#include <Processors/QueryPlan/CachingStep.h>
#include <Processors/Transforms/CachingTransform.h>
#include <Parsers/IAST.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

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
    // only returns true for one thread, so that multiple threads are not caching the same query result at the same time
    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
        { return std::make_shared<CachingTransform>(header, cache, cache_key); });
}

CachingStep::CachingStep(const DataStream & input_stream_, QueryCachePtr cache_, CacheKey cache_key_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , cache(cache_)
    , cache_key(cache_key_)
{
}

}
