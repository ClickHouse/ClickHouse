#include <type_traits>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static Block checkHeaders(const Headers & input_headers)
{
    if (input_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite an empty set of query plan steps");

    Block res = input_headers.front();
    for (const auto & header : input_headers)
        assertBlocksHaveEqualStructure(header, res, "UnionStep");

    return res;
}

UnionStep::UnionStep(Headers input_headers_, size_t max_threads_)
    : max_threads(max_threads_)
{
    updateInputHeaders(std::move(input_headers_));
}

void UnionStep::updateOutputHeader()
{
    output_header = checkHeaders(input_headers);
}

QueryPipelineBuilderPtr UnionStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(*output_header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    size_t new_max_threads = max_threads ? max_threads : settings.max_threads;

    for (auto & cur_pipeline : pipelines)
    {
#if !defined(NDEBUG)
        assertCompatibleHeader(cur_pipeline->getHeader(), getOutputHeader(), "UnionStep");
#endif
        /// Headers for union must be equal.
        /// But, just in case, convert it to the same header if not.
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
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), new_max_threads, &processors);
    return pipeline;
}

void UnionStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void UnionStep::serialize(Serialization & ctx) const
{
    (void)ctx;
}

std::unique_ptr<IQueryPlanStep> UnionStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<UnionStep>(ctx.input_headers);
}

void registerUnionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Union", &UnionStep::deserialize);
}

}
