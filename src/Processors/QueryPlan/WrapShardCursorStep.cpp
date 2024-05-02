#include <memory>

#include <Processors/QueryPlan/WrapShardCursorStep.h>
#include <Processors/Transforms/WrapShardCursorTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

WrapShardCursorStep::WrapShardCursorStep(
    const DataStream & input_stream_, size_t shard_num_, ShardCursorChanges changes_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , shard_num(shard_num_)
    , changes(std::move(changes_))
{
}

void WrapShardCursorStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header)
                                { return std::make_shared<WrapShardCursorTransform>(header, shard_num, changes); });
}

void WrapShardCursorStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}

}
