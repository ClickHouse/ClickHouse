#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/JoiningTransform.h>

namespace DB
{

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    bool has_non_joined_rows_,
    size_t max_block_size_)
    : IQueryPlanStep()
    , join(std::move(join_))
    , has_non_joined_rows(has_non_joined_rows_)
    , max_block_size(max_block_size_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(left_stream_.header, join),
    };
}

QueryPipelinePtr JoinStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    auto left_pipeline = std::move(pipelines[0]);
    auto right_pipeline = std::move(pipelines[1]);

    /// In case joined subquery has totals, and we don't, add default chunk to totals.
    bool add_default_totals = false;
    if (!left_pipeline->hasTotals() && right_pipeline->hasTotals())
    {
        left_pipeline->addDefaultTotals();
        add_default_totals = true;
    }

    JoiningTransform::FinishCounterPtr finish_counter;
    if (has_non_joined_rows)
        finish_counter = std::make_shared<JoiningTransform::FinishCounter>(left_pipeline->getNumStreams());

    right_pipeline->resize(1);

    auto adding_joined = std::make_shared<AddingJoinedTransform>(right_pipeline->getHeader(), join);
    InputPort * totals_port = nullptr;
    if (right_pipeline->hasTotals())
        totals_port = adding_joined->addTotaslPort();

    right_pipeline->addTransform(std::move(adding_joined), totals_port, nullptr);
    right_pipeline->resize(left_pipeline->getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<JoiningTransform>(header, join, max_block_size, on_totals, add_default_totals);
    });

    if (has_non_joined_rows)
    {
        const Block & join_result_sample = pipeline.getHeader();
        auto stream = std::make_shared<LazyNonJoinedBlockInputStream>(*join, join_result_sample, max_block_size);
        auto source = std::make_shared<SourceFromInputStream>(std::move(stream));

        source->setQueryPlanStep(this);
        pipeline.addDelayedStream(source);

        /// Now, after adding delayed stream, it has implicit dependency on other port.
        /// Here we add resize processor to remove this dependency.
        /// Otherwise, if we add MergeSorting + MergingSorted transform to pipeline, we could get `Pipeline stuck`
        pipeline.resize(pipeline.getNumStreams(), true);
    }
}

}
