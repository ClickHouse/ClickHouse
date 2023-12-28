#include <memory>

#include <Interpreters/ActionsDAG.h>

#include <Processors/QueryPlan/StreamingAdapterStep.h>
#include <Processors/Streaming/StreamingAdapter.h>
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
    Block res = storage_stream.header;
    assertBlocksHaveEqualStructure(subscription_stream.header, res, "StreamingAdapterStep");

    return res;
}

StreamingAdapterStep::StreamingAdapterStep(DataStream storage_stream, DataStream subscription_stream)
    : storage_header(checkHeaders(storage_stream, subscription_stream))
{
    updateInputStreams({std::move(storage_stream), std::move(subscription_stream)});
}

QueryPipelineBuilderPtr StreamingAdapterStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected storage and subscription pipelines in StreamingAdapterStep::updatePipeline");

    /// resize pipelines to 1 output port as preparation for fifo order
    for (auto & cur_pipeline : pipelines)
        cur_pipeline->resize(1);

    auto streaming_adapter = std::make_shared<StreamingAdapter>(storage_header);

    return QueryPipelineBuilder::mergePipelines(
        std::move(pipelines[0]), std::move(pipelines[1]), std::move(streaming_adapter), &processors);
}

void StreamingAdapterStep::updateOutputStream()
{
    output_stream = DataStream{
        .header = storage_header,
        .has_single_port = true,
    };
}

void StreamingAdapterStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
