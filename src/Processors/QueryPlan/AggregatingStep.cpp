#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>

namespace DB
{

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    AggregatingTransformParamsPtr transform_params_,
    size_t max_block_size_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_,
    InputOrderInfoPtr group_by_info_,
    SortDescription group_by_sort_description_)
    : ITransformingStep(input_stream_, input_stream_)
    , transform_params(std::move(transform_params_))
    , max_block_size(max_block_size_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
{
}

void AggregatingStep::transformPipeline(QueryPipeline & pipeline)
{
    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

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
                auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
                size_t counter = 0;
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(header, transform_params, group_by_sort_description, max_block_size, many_data, counter++);
                });

                for (auto & column_description : group_by_sort_description)
                {
                    if (!column_description.column_name.empty())
                    {
                        column_description.column_number = pipeline.getHeader().getPositionByName(column_description.column_name);
                        column_description.column_name.clear();
                    }
                }

                auto transform = std::make_shared<AggregatingSortedTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    group_by_sort_description,
                    max_block_size);

                pipeline.addPipe({ std::move(transform) });
            }
            else
            {
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(header, transform_params, group_by_sort_description, max_block_size);
                });
            }

            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FinalizingSimpleTransform>(header, transform_params);
            });

            pipeline.enableQuotaForCurrentStreams();
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

        pipeline.resize(1);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params);
        });
    }

    pipeline.enableQuotaForCurrentStreams();
}

}
