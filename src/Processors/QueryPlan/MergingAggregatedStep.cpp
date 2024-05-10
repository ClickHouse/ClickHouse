#include <Interpreters/Context.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MemoryBoundMerging.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static bool memoryBoundMergingWillBeUsed(
    const DataStream & input_stream,
    bool memory_bound_merging_of_aggregation_results_enabled,
    const SortDescription & group_by_sort_description)
{
    return memory_bound_merging_of_aggregation_results_enabled && !group_by_sort_description.empty()
        && input_stream.sort_scope >= DataStream::SortScope::Stream && input_stream.sort_description.hasPrefix(group_by_sort_description);
}

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = should_produce_results_in_order_of_bucket_number,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

MergingAggregatedStep::MergingAggregatedStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_,
    bool should_produce_results_in_order_of_bucket_number_,
    size_t max_block_size_,
    size_t memory_bound_merging_max_block_bytes_,
    SortDescription group_by_sort_description_,
    bool memory_bound_merging_of_aggregation_results_enabled_)
    : ITransformingStep(
        input_stream_,
        params_.getHeader(input_stream_.header, final_),
        getTraits(should_produce_results_in_order_of_bucket_number_))
    , params(std::move(params_))
    , final(final_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(max_threads_)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
    , max_block_size(max_block_size_)
    , memory_bound_merging_max_block_bytes(memory_bound_merging_max_block_bytes_)
    , group_by_sort_description(std::move(group_by_sort_description_))
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
    , memory_bound_merging_of_aggregation_results_enabled(memory_bound_merging_of_aggregation_results_enabled_)
{
    if (memoryBoundMergingWillBeUsed() && should_produce_results_in_order_of_bucket_number)
    {
        output_stream->sort_description = group_by_sort_description;
        output_stream->sort_scope = DataStream::SortScope::Global;
    }
}

void MergingAggregatedStep::applyOrder(SortDescription sort_description, DataStream::SortScope sort_scope)
{
    is_order_overwritten = true;
    overwritten_sort_scope = sort_scope;

    auto & input_stream = input_streams.front();
    input_stream.sort_scope = sort_scope;
    input_stream.sort_description = sort_description;

    /// Columns might be reordered during optimization, so we better to update sort description.
    group_by_sort_description = std::move(sort_description);

    if (memoryBoundMergingWillBeUsed() && should_produce_results_in_order_of_bucket_number)
    {
        output_stream->sort_description = group_by_sort_description;
        output_stream->sort_scope = DataStream::SortScope::Global;
    }
}

void MergingAggregatedStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);

    if (memoryBoundMergingWillBeUsed())
    {
        auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            transform_params,
            group_by_sort_description,
            max_block_size,
            memory_bound_merging_max_block_bytes);

        pipeline.addTransform(std::move(transform));

        /// Do merge of aggregated data in parallel.
        pipeline.resize(max_threads);

        const auto & required_sort_description
            = should_produce_results_in_order_of_bucket_number ? group_by_sort_description : SortDescription{};

        pipeline.addSimpleTransform(
            [&](const Block &) { return std::make_shared<MergingAggregatedBucketTransform>(transform_params, required_sort_description); });

        if (should_produce_results_in_order_of_bucket_number)
        {
            pipeline.addTransform(
                std::make_shared<SortingAggregatedForMemoryBoundMergingTransform>(pipeline.getHeader(), pipeline.getNumStreams()));
        }

        return;
    }

    if (!memory_efficient_aggregation)
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
                                    { return std::make_shared<MergingAggregatedTransform>(header, transform_params, max_threads); });
    }
    else
    {
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? memory_efficient_merge_threads
                                 : max_threads;

        pipeline.addMergingAggregatedMemoryEfficientTransform(transform_params, num_merge_threads);
    }

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

void MergingAggregatedStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void MergingAggregatedStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void MergingAggregatedStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), params.getHeader(input_streams.front().header, final), getDataStreamTraits());
    if (is_order_overwritten)  /// overwrite order again
        applyOrder(group_by_sort_description, overwritten_sort_scope);
}

bool MergingAggregatedStep::memoryBoundMergingWillBeUsed() const
{
    return DB::memoryBoundMergingWillBeUsed(
        input_streams.front(), memory_bound_merging_of_aggregation_results_enabled, group_by_sort_description);
}
}
