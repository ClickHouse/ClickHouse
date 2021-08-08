#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/ResizeProcessor.h>


namespace DB
{

Block IntersectOrExceptStep::checkHeaders(const DataStreams & input_streams_) const
{
    if (input_streams_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot perform {} on empty set of query plan steps", getName());

    Block res = input_streams_.front().header;
    for (const auto & stream : input_streams_)
        assertBlocksHaveEqualStructure(stream.header, res, "IntersectOrExceptStep");

    return res;
}

IntersectOrExceptStep::IntersectOrExceptStep(bool is_except_, DataStreams input_streams_, size_t max_threads_)
    : is_except(is_except_), header(checkHeaders(input_streams_)), max_threads(max_threads_)
{
    input_streams = std::move(input_streams_);
    if (input_streams.size() == 1)
        output_stream = input_streams.front();
    else
        output_stream = DataStream{.header = header};
}

QueryPipelinePtr IntersectOrExceptStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    if (pipelines.empty())
    {
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    for (auto & cur_pipeline : pipelines)
    {
        /// Just in case.
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

    *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), max_threads);
    pipeline->addTransform(std::make_shared<IntersectOrExceptTransform>(is_except, header));

    processors = collector.detachProcessors();
    return pipeline;
}

void IntersectOrExceptStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
