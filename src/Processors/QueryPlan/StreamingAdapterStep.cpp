#include <memory>

#include <Interpreters/ActionsDAG.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/StreamingAdapterStep.h>
#include <Processors/StreamingAdapter.h>
#include <Processors/Transforms/ExpressionTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static Block checkHeaders(DataStream storage_stream, DataStream subscription_stream)
{
    assertBlocksHaveEqualStructure(subscription_stream.header, storage_stream.header, "StreamingAdapterStep");
    return storage_stream.header;
}

StreamingAdapterStep::StreamingAdapterStep(DataStream subscription_stream_)
    : output_header(subscription_stream_.header)
{
    updateInputStreams({std::move(subscription_stream_)});
}

StreamingAdapterStep::StreamingAdapterStep(DataStream storage_stream_, DataStream subscription_stream_)
    : output_header(checkHeaders(storage_stream_, subscription_stream_))
{
    updateInputStreams({std::move(storage_stream_), std::move(subscription_stream_)});
}

QueryPipelineBuilderPtr StreamingAdapterStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (input_streams.size() == 1 && pipelines.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected single subscription pipeline in StreamingAdapterStep::updatePipeline");

    if (input_streams.size() == 2 && pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected storage and subscription pipelines in StreamingAdapterStep::updatePipeline");

    /// simple case when step is used only to update DataStream with infinite flag
    if (pipelines.size() == 1)
        return std::move(pipelines[0]);

    /// resize pipelines to 1 output port as preparation for fifo order
    for (auto & cur_pipeline : pipelines)
        cur_pipeline->resize(1);

    auto streaming_adapter = std::make_shared<StreamingAdapter>(output_header);

    return QueryPipelineBuilder::mergePipelines(
        std::move(pipelines[0]), std::move(pipelines[1]), std::move(streaming_adapter), &processors);
}

void StreamingAdapterStep::updateOutputStream()
{
    output_stream = DataStream{
        .header = output_header,
        .has_single_port = input_streams.size() == 1 ? input_streams.front().has_single_port : true,
        .is_infinite = true,
    };
}

void StreamingAdapterStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void makeStreamInfinite(QueryPlan & plan)
{
    plan.addStep(std::make_unique<StreamingAdapterStep>(plan.getCurrentDataStream()));
}

}
