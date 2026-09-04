#include <Processors/QueryPlan/WriteToQueryResultCacheStep.h>
#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

WriteToQueryResultCacheStep::WriteToQueryResultCacheStep(
    const SharedHeader & input_header_,
    std::shared_ptr<QueryResultCacheWriter> cache_writer_)
    : ITransformingStep(
        input_header_,
        input_header_,
        Traits{
            .data_stream_traits = {
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            .transform_traits = {
                .preserves_number_of_rows = true,
            },
        })
    , cache_writer(std::move(cache_writer_))
{
}

void WriteToQueryResultCacheStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & header, Pipe::StreamType stream_type)
    {
        QueryResultCacheWriter::ChunkType chunk_type;
        switch (stream_type)
        {
            case Pipe::StreamType::Totals:
                chunk_type = QueryResultCacheWriter::ChunkType::Totals;
                break;
            case Pipe::StreamType::Extremes:
                chunk_type = QueryResultCacheWriter::ChunkType::Extremes;
                break;
            default:
                chunk_type = QueryResultCacheWriter::ChunkType::Result;
                break;
        }
        return std::make_shared<StreamInQueryResultCacheTransform>(
            *header, cache_writer, chunk_type);
    });
}

}
