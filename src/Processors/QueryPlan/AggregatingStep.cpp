#include <Processors/QueryPlan/AggregatingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    size_t max_block_size_,
    size_t aggregation_in_order_max_block_bytes_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_,
    InputOrderInfoPtr group_by_info_,
    SortDescription group_by_sort_description_,
    bool optimize_distributed_aggregation_,
    ContextMutablePtr context_)
    : ITransformingStep(input_stream_, params_.getHeader(final_, optimize_distributed_aggregation_ && params_.max_threads > 1), getTraits(), false)
    , params(std::move(params_))
    , final(std::move(final_))
    , max_block_size(max_block_size_)
    , aggregation_in_order_max_block_bytes(aggregation_in_order_max_block_bytes_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
    , optimize_distributed_aggregation(optimize_distributed_aggregation_)
    , context(context_)
{
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final);

    if (group_by_info)
    {
        bool need_finish_sorting = (group_by_info->order_key_prefix_descr.size() < group_by_sort_description.size());

        if (need_finish_sorting)
        {
            /// TOO SLOW
        }
        else
        {
            if (pipeline.getNumStreams() > 1)
            {
                /** The pipeline is the following:
                 *
                 * --> AggregatingInOrder                                                  --> MergingAggregatedBucket
                 * --> AggregatingInOrder --> FinishAggregatingInOrder --> ResizeProcessor --> MergingAggregatedBucket
                 * --> AggregatingInOrder                                                  --> MergingAggregatedBucket
                 */

                auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
                size_t counter = 0;
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    /// We want to merge aggregated data in batches of size
                    /// not greater than 'aggregation_in_order_max_block_bytes'.
                    /// So, we reduce 'max_bytes' value for aggregation in 'merge_threads' times.
                    return std::make_shared<AggregatingInOrderTransform>(
                        header, transform_params, group_by_sort_description,
                        max_block_size, aggregation_in_order_max_block_bytes / merge_threads,
                        many_data, counter++);
                });

                aggregating_in_order = collector.detachProcessors(0);

                auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    transform_params,
                    group_by_sort_description,
                    max_block_size,
                    aggregation_in_order_max_block_bytes);

                pipeline.addTransform(std::move(transform));

                /// Do merge of aggregated data in parallel.
                pipeline.resize(merge_threads);

                pipeline.addSimpleTransform([&](const Block &)
                {
                    return std::make_shared<MergingAggregatedBucketTransform>(transform_params);
                });

                aggregating_sorted = collector.detachProcessors(1);
            }
            else
            {
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(
                        header, transform_params, group_by_sort_description,
                        max_block_size, aggregation_in_order_max_block_bytes);
                });

                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<FinalizeAggregatedTransform>(header, transform_params);
                });

                aggregating_in_order = collector.detachProcessors(0);
            }

            finalizing = collector.detachProcessors(2);
            return;
        }
    }

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });

        aggregating = collector.detachProcessors(0);

        pipeline.resize(1);

        if (optimize_distributed_aggregation)
        {
            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<AppendFinalizedTransform>(header, params.getHeader(final, optimize_distributed_aggregation));
            });

            auto holder = AggregatingMemoryHolder(many_data, transform_params);
            context->getAggregatingMemoryCallback()(holder);
        }
    }
    else
    {
        pipeline.resize(1);

        auto many_data = std::make_shared<ManyAggregatedData>(1);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params, many_data);
        });

        aggregating = collector.detachProcessors(0);

        if (optimize_distributed_aggregation)
        {
            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<AppendFinalizedTransform>(header, params.getHeader(final, optimize_distributed_aggregation));
            });

            auto holder = AggregatingMemoryHolder(many_data, transform_params);
            context->getAggregatingMemoryCallback()(holder);
        }

        // pipeline.addSimpleTransform([&](const Block & header)
        // {
        //     return std::make_shared<LookupingTransform>(header, params.getHeader(final, false), transform_params, holder);
        // });
    }
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    if (!aggregating.empty())
        IQueryPlanStep::describePipeline(aggregating, settings);
    else
    {
        /// Processors are printed in reverse order.
        IQueryPlanStep::describePipeline(finalizing, settings);
        IQueryPlanStep::describePipeline(aggregating_sorted, settings);
        IQueryPlanStep::describePipeline(aggregating_in_order, settings);
    }
}

}
