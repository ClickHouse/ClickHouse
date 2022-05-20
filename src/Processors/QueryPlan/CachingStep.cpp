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
    if (cache->getPutInCacheMutex(cache_key).try_lock())
    {
        execute_caching = true;
        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
            { return std::make_shared<CachingTransform>(header, cache, cache_key.ast, cache_key.settings, cache_key.username); });
    }
}

CachingStep::~CachingStep()
{
    if (execute_caching)
    {
        cache->getPutInCacheMutex(cache_key).unlock();
        cache->scheduleRemoval(cache_key);
    }
}

CachingStep::CachingStep(const DataStream & input_stream_, QueryCachePtr cache_, CacheKey cache_key_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , cache(cache_)
    , cache_key(std::move(cache_key_))
{}

}
