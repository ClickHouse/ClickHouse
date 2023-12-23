#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Processors/QueryPlan/StreamingAdapterStep.h>
#include <Processors/StreamingAdapter.h>

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
StreamingAdapterStep::StreamingAdapterStep(const DataStream & input_stream, Block sample, SubscriberPtr sub)
    : ITransformingStep(input_stream, input_stream.header, getTraits()), storage_sample(std::move(sample)), subscriber(std::move(sub))
{
}

void StreamingAdapterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<StreamingAdapter>(
        pipeline.getHeader(), pipeline.getNumStreams(), std::move(storage_sample), std::move(subscriber));

    pipeline.addTransform(std::move(transform));
}

}
