#include <Processors/QueryPlan/UnionStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <base/defines.h>

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

QueryPipelineBuilderPtr UnionStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    if (pipelines.empty())
    {
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    for (auto & cur_pipeline : pipelines)
    {
#if !defined(NDEBUG)
        assertCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header, "UnionStep");
#endif
        /// Headers for union must be equal.
        /// But, just in case, convert it to the same header if not.
        if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputStream().header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform([&](const Block & cur_header)
            {
                return std::make_shared<ExpressionTransform>(cur_header, converting_actions);
            });
        }
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), max_threads);

    processors = collector.detachProcessors();
    return pipeline;
}

void UnionStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
