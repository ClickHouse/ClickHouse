#include <Processors/QueryPlan/StreamInQueryCacheStep.h>
#include <Processors/Transforms/StreamInQueryCacheTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }};
}

StreamInQueryCacheStep::StreamInQueryCacheStep(const Header & input_header_, std::shared_ptr<QueryCacheWriter> writer_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , writer(writer_)
{
}

void StreamInQueryCacheStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            using ChunkType = QueryCacheWriter::ChunkType;
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

            return std::make_shared<StreamInQueryCacheTransform>(header, writer, chunk_type);
        });
}

}
