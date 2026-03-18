#include <Processors/QueryPlan/IntersectOrExceptStep.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/ResizeProcessor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static Block checkHeaders(const Headers & input_headers)
{
    if (input_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot perform intersect/except on empty set of query plan steps");

    Block res = input_headers.front();
    for (const auto & header : input_headers)
        assertBlocksHaveEqualStructure(header, res, "IntersectOrExceptStep");

    return res;
}

IntersectOrExceptStep::IntersectOrExceptStep(
    Headers input_headers_, Operator operator_, size_t max_threads_)
    : current_operator(operator_)
    , max_threads(max_threads_)
{
    updateInputHeaders(std::move(input_headers_));
}

void IntersectOrExceptStep::updateOutputHeader()
{
    output_header = checkHeaders(input_headers);
}

QueryPipelineBuilderPtr IntersectOrExceptStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(*output_header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    for (auto & cur_pipeline : pipelines)
    {
        /// Just in case.
        if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputHeader()))
        {
            QueryPipelineProcessorsCollector collector(*cur_pipeline, this);
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputHeader().getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform([&](const Block & cur_header)
            {
                return std::make_shared<ExpressionTransform>(cur_header, converting_actions);
            });

            auto added_processors = collector.detachProcessors();
            processors.insert(processors.end(), added_processors.begin(), added_processors.end());
        }

        /// For the case of union.
        cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(getOutputHeader(), cur_pipeline->getNumStreams(), 1));
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), max_threads, &processors);
    auto transform = std::make_shared<IntersectOrExceptTransform>(getOutputHeader(), current_operator);
    processors.push_back(transform);
    pipeline->addTransform(std::move(transform));

    return pipeline;
}

void IntersectOrExceptStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
