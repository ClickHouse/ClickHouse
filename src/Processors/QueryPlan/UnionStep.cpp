#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/NullSource.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static Block checkHeaders(const DataStreams & input_streams)
{
    if (input_streams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite an empty set of query plan steps");

    Block res = input_streams.front().header;
    for (const auto & stream : input_streams)
        assertBlocksHaveEqualStructure(stream.header, res, "UnionStep");

    return res;
}

UnionStep::UnionStep(DataStreams input_streams_, size_t max_threads_)
    : header(checkHeaders(input_streams_))
    , max_threads(max_threads_)
{
    input_streams = std::move(input_streams_);

    if (input_streams.size() == 1)
        output_stream = input_streams.front();
    else
        output_stream = DataStream{.header = header};
}

QueryPipelinePtr UnionStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    if (pipelines.empty())
    {
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), max_threads);

    processors = collector.detachProcessors();
    return pipeline;
}

void UnionStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
