#include <cassert>
#include <cstddef>
#include <memory>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include "Interpreters/Aggregator.h"
#include "Processors/QueryPlan/IQueryPlanStep.h"
#include "Processors/Transforms/GroupingSetsTransform.h"

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
    SortDescription group_by_sort_description_)
    : ITransformingStep(input_stream_, params_.getHeader(final_), getTraits(), false)
    , params(std::move(params_))
    , final(std::move(final_))
    , max_block_size(max_block_size_)
    , aggregation_in_order_max_block_bytes(aggregation_in_order_max_block_bytes_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
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

    if (params.hasGroupingSets())
    {
        const auto & grouping_sets_params = params.getGroupingSetsParams();
        const size_t grouping_sets_size = grouping_sets_params.size();

        const size_t streams = pipeline.getNumStreams();

        auto input_header = pipeline.getHeader();
        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            Processors copiers;
            copiers.reserve(ports.size());

            for (auto * port : ports)
            {
                auto copier = std::make_shared<CopyTransform>(input_header, grouping_sets_size);
                connect(*port, copier->getInputPort());
                copiers.push_back(copier);
            }

            return copiers;
        });

        std::vector<AggregatingTransformParamsPtr> transform_params_per_set;
        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            assert(streams * grouping_sets_size == ports.size());
            Processors aggregators;
            for (size_t i = 0; i < grouping_sets_size; ++i)
            {
                Aggregator::Params params_for_set
                {
                    transform_params->params.src_header,
                    grouping_sets_params.grouping_sets_with_keys[i],
                    transform_params->params.aggregates,
                    transform_params->params.overflow_row,
                    transform_params->params.max_rows_to_group_by,
                    transform_params->params.group_by_overflow_mode,
                    transform_params->params.group_by_two_level_threshold,
                    transform_params->params.group_by_two_level_threshold_bytes,
                    transform_params->params.max_bytes_before_external_group_by,
                    transform_params->params.empty_result_for_aggregation_by_empty_set,
                    transform_params->params.tmp_volume,
                    transform_params->params.max_threads,
                    transform_params->params.min_free_disk_space,
                    transform_params->params.compile_aggregate_expressions,
                    transform_params->params.min_count_to_compile_aggregate_expression,
                    transform_params->params.intermediate_header,
                    transform_params->params.stats_collecting_params
                };
                auto transform_params_for_set = std::make_shared<AggregatingTransformParams>(std::move(params_for_set), final);
                transform_params_per_set.push_back(transform_params_for_set);

                if (streams > 1)
                {
                    auto many_data = std::make_shared<ManyAggregatedData>(streams);
                    for (size_t j = 0; j < streams; ++j)
                    {
                        auto aggregation_for_set = std::make_shared<AggregatingTransform>(input_header, transform_params_for_set, many_data, j, merge_threads, temporary_data_merge_threads);
                        connect(*ports[i + streams * j], aggregation_for_set->getInputs().front());
                        aggregators.push_back(aggregation_for_set);
                    }
                }
                else
                {
                    auto aggregation_for_set = std::make_shared<AggregatingTransform>(input_header, transform_params_for_set);
                    connect(*ports[i], aggregation_for_set->getInputs().front());
                    aggregators.push_back(aggregation_for_set);
                }
            }
            return aggregators;
        }, false);

        if (streams > 1)
        {
            pipeline.transform([&](OutputPortRawPtrs ports)
            {
                Processors resizes;
                for (size_t i = 0; i < grouping_sets_size; ++i)
                {
                    auto resize = std::make_shared<ResizeProcessor>(transform_params_per_set[i]->getHeader(), streams, 1);
                    auto & inputs = resize->getInputs();
                    auto output_it = ports.begin() + i * streams;
                    for (auto input_it = inputs.begin(); input_it != inputs.end(); ++output_it, ++input_it)
                        connect(**output_it, *input_it);
                    resizes.push_back(resize);
                }
                return resizes;
            }, false);
        }

        assert(pipeline.getNumStreams() == grouping_sets_size);
        size_t set_counter = 0;
        auto output_header = transform_params->getHeader();
        pipeline.addSimpleTransform([&](const Block & header)
        {
            auto transform = std::make_shared<GroupingSetsTransform>(header, output_header, transform_params_per_set[set_counter], grouping_sets_params.missing_columns_per_set, set_counter);
            ++set_counter;
            return transform;
        });

        aggregating = collector.detachProcessors(0);
        return;
    }
    else
    {
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

            pipeline.resize(1);

            aggregating = collector.detachProcessors(0);
        }
        else
        {
            pipeline.resize(1);

            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<AggregatingTransform>(header, transform_params);
            });

            aggregating = collector.detachProcessors(0);
        }
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
