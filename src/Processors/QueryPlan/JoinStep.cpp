#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>

#include <base/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(left_stream_.header, join),
    };
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    return QueryPipelineBuilder::joinPipelines(std::move(pipelines[0]), std::move(pipelines[1]), join, max_block_size, max_streams, keep_left_read_in_order, &processors);
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_stream_,
        JoiningTransform::transformHeader(input_stream_.header, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, join, max_block_size, on_totals, default_totals, counter);
    });
}

ParallelJoinStep::ParallelJoinStep(DataStreams input_streams_, JoinPtr join_, size_t /*max_threads_*/)
    :  join(std::move(join_))
//    , max_threads(max_threads_)
{
    input_streams = std::move(input_streams_);

    Block joined_header;

    // const auto & an_input_stream = input_streams.front();

    for (const auto & an_input_stream : input_streams)
    {
        const auto & current = an_input_stream.header;
        for (auto it = current.begin(); it != current.end(); ++it)
        {
            joined_header.insert(*it);
        }
    }

    LOG_TRACE(&Poco::Logger::get("ParallelJoinStep"), "ctor: header structure {}", joined_header.dumpStructure());

    output_stream = DataStream
    {
        .header = joined_header // JoiningTransform::transformHeader(left_stream_.header, join),
    };

    LOG_TRACE(&Poco::Logger::get("ParallelJoinStep"), "ctor: output_stream header structure {}", output_stream->header.dumpStructure());
}

QueryPipelineBuilderPtr ParallelJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    return QueryPipelineBuilder::parallelJoinPipelines(std::move(pipelines), join, max_block_size, &processors);
}

void ParallelJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
