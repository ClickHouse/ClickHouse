#include <Processors/QueryPlan/StreamInQueryResultCacheStep.h>
#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Cache/QueryResultCache.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

StreamInQueryResultCacheStep::StreamInQueryResultCacheStep(
    const Header & input_header_,
    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , query_result_cache_writer(query_result_cache_writer_)
{
}

void StreamInQueryResultCacheStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            using ChunkType = QueryResultCacheWriter::ChunkType;
            using StreamType = QueryPipelineBuilder::StreamType;

            ChunkType chunk_type;

            switch (stream_type)
            {
                case StreamType::Main:
                {
                    chunk_type = ChunkType::Result;
                    break;
                }
                case StreamType::Totals:
                {
                    chunk_type = ChunkType::Totals;
                    break;
                }
                case StreamType::Extremes:
                {
                    chunk_type = ChunkType::Extremes;
                    break;
                }
            }

            return std::make_shared<StreamInQueryResultCacheTransform>(header, query_result_cache_writer, chunk_type);
        });
}

}
