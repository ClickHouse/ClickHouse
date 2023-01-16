#include <type_traits>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/UnionStep.h>
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

    updateOutputSortDescription();
}

void UnionStep::updateOutputSortDescription()
{
    SortDescription common_sort_description = input_streams.front().sort_description;
    DataStream::SortScope sort_scope = input_streams.front().sort_scope;
    for (const auto & input_stream : input_streams)
    {
        common_sort_description = commonPrefix(common_sort_description, input_stream.sort_description);
        sort_scope = std::min(sort_scope, input_stream.sort_scope);
    }
    if (!common_sort_description.empty() && sort_scope >= DataStream::SortScope::Chunk)
    {
        output_stream->sort_description = common_sort_description;
        if (sort_scope == DataStream::SortScope::Global && input_streams.size() > 1)
            output_stream->sort_scope = DataStream::SortScope::Stream;
        else
            output_stream->sort_scope = sort_scope;
    }
}

QueryPipelineBuilderPtr UnionStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
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
            QueryPipelineProcessorsCollector collector(*cur_pipeline, this);
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputStream().header.getColumnsWithTypeAndName(),
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

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), max_threads, &processors);
    return pipeline;
}

void UnionStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
