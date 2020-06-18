#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = false
    };
}

MergingAggregatedStep::MergingAggregatedStep(
    const DataStream & input_stream_,
    AggregatingTransformParamsPtr params_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_)
    : ITransformingStep(input_stream_, params_->getHeader(), getTraits())
    , params(params_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(max_threads_)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
{
    /// Aggregation keys are distinct
    for (auto key : params->params.keys)
        output_stream->distinct_columns.insert(params->params.src_header.getByPosition(key).name);
}

void MergingAggregatedStep::transformPipeline(QueryPipeline & pipeline)
{
    if (!memory_efficient_aggregation)
    {
        /// We union several sources into one, parallelizing the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<MergingAggregatedTransform>(header, params, max_threads);
        });
    }
    else
    {
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? static_cast<size_t>(memory_efficient_merge_threads)
                                 : static_cast<size_t>(max_threads);

        auto pipe = createMergingAggregatedMemoryEfficientPipe(
                pipeline.getHeader(),
                params,
                pipeline.getNumStreams(),
                num_merge_threads);

        pipeline.addPipe(std::move(pipe));
    }

    pipeline.enableQuotaForCurrentStreams();
}

}
