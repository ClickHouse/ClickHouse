#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPlan/Cuda/CudaAggregatingStep.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/Cuda/CudaAggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = should_produce_results_in_order_of_bucket_number, /// Actually, may also return single stream if should_produce_results_in_order_of_bucket_number = false
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

static Block appendGroupingColumn(Block block, const GroupingSetsParamsList & /*params*/)
{
    // if (params.empty())
    return block;

    // Block res;

    // size_t rows = block.rows();
    // auto column = ColumnUInt64::create(rows);

    // res.insert({ColumnPtr(std::move(column)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    // for (auto & col : block)
    //     res.insert(std::move(col));

    // return res;
}

CudaAggregatingStep::CudaAggregatingStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    GroupingSetsParamsList grouping_sets_params_,
    bool final_,
    size_t /*max_block_size_*/,
    size_t /*aggregation_in_order_max_block_bytes_*/,
    size_t merge_threads_,
    size_t /*temporary_data_merge_threads_*/,
    bool /*storage_has_evenly_distributed_read_*/,
    InputOrderInfoPtr group_by_info_,
    SortDescription group_by_sort_description_,
    bool should_produce_results_in_order_of_bucket_number_,
    ContextPtr context_)
    : ITransformingStep(
        input_stream_, appendGroupingColumn(params_.getHeader(input_stream_.header, final_), grouping_sets_params_), getTraits(should_produce_results_in_order_of_bucket_number_), false)
    , context(context_)
    , params(std::move(params_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , final(std::move(final_))
    , merge_threads(merge_threads_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
    // , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
{
}

void CudaAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = false; //pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    const auto src_header = pipeline.getHeader();
    auto transform_params = std::make_shared<CudaAggregatingTransformParams>(src_header, std::move(params), final, context);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        // pipeline.resize(1, true, true);

        // if (!storage_has_evenly_distributed_read)
        //     pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<CudaAggregatedDataVariants>(pipeline.getNumStreams());

        size_t counter = 0;

        pipeline.addSimpleTransform(
            [&](const Block & header)
            {
                return std::make_shared<CudaAggregatingTransform>(
                    header, transform_params, many_data, counter++, merge_threads, context); //, temporary_data_merge_threads);
            });

        pipeline.resize(1);

        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header)
                                    { return std::make_shared<CudaAggregatingTransform>(header, transform_params, context); });

        aggregating = collector.detachProcessors(0);
    }
}

void CudaAggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void CudaAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void CudaAggregatingStep::describePipeline(FormatSettings & settings) const
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

void CudaAggregatingStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        appendGroupingColumn(params.getHeader(input_streams.front().header, final), grouping_sets_params),
        getDataStreamTraits());
}


}
