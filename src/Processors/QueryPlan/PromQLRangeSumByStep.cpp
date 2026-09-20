#include <Processors/QueryPlan/PromQLRangeSumByStep.h>

#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/PromQLPartialGroupMergeTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

}

PromQLRangeSumByStep::PromQLRangeSumByStep(
    SharedHeader input_header_,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    AggregateFunctionPtr sum_function_,
    Strings labels_to_keep_,
    size_t max_samples_per_series_,
    size_t max_output_groups_,
    size_t max_output_block_size_,
    bool parallel_processing_requested_,
    size_t max_parallel_lanes_)
    : ITransformingStep(input_header_, PromQLRangeSumByTransform::transformHeader(sum_function_), getTraits())
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , sum_function(std::move(sum_function_))
    , labels_to_keep(std::move(labels_to_keep_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_groups(max_output_groups_)
    , max_output_block_size(max_output_block_size_)
    , max_parallel_lanes(max_parallel_lanes_)
    , parallel_processing_requested(parallel_processing_requested_)
{
}

void PromQLRangeSumByStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremes();

    if (pipeline.getNumStreams() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native range sum requires at least one ordered input stream");

    if (parallel_processing_enabled && pipeline.getNumStreams() > 1)
    {
        auto group_limit = std::make_shared<PromQLGroupLimit>(max_output_groups);
        pipeline.addSimpleTransform(
            [collector_ptr = collector,
             rate_function_ptr = rate_function,
             sum_function_ptr = sum_function,
             labels = labels_to_keep,
             max_samples = max_samples_per_series,
             max_groups = max_output_groups,
             output_block_size = max_output_block_size,
             group_limit](const SharedHeader & header)
            {
                return std::make_shared<PromQLRangeSumByTransform>(
                    header,
                    collector_ptr,
                    rate_function_ptr,
                    sum_function_ptr,
                    labels,
                    max_samples,
                    max_groups,
                    output_block_size,
                    group_limit);
            });

        pipeline.resize(1);
        pipeline.addSimpleTransform(
            [sum_function_ptr = sum_function,
             max_groups = max_output_groups,
             output_block_size = max_output_block_size,
             group_limit](const SharedHeader & header)
            {
                return std::make_shared<PromQLPartialGroupMergeTransform>(
                    header, sum_function_ptr, max_groups, output_block_size, group_limit);
            });
        return;
    }

    if (pipeline.getNumStreams() > 1)
    {
        const auto & header = *pipeline.getSharedHeader();
        if (!header.has(TimeSeriesColumnNames::ID) || !header.has(TimeSeriesColumnNames::Bucket))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PromQL native range sum requires {} and {} columns to merge {} ordered input streams",
                TimeSeriesColumnNames::ID,
                TimeSeriesColumnNames::Bucket,
                pipeline.getNumStreams());

        SortDescription description;
        description.emplace_back(TimeSeriesColumnNames::ID, 1, 1);
        description.emplace_back(TimeSeriesColumnNames::Bucket, 1, 1);
        auto merge = std::make_shared<MergingSortedTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            description,
            max_output_block_size,
            /*max_block_size_bytes=*/0,
            /*max_dynamic_subcolumns=*/std::nullopt,
            SortingQueueStrategy::Batch);
        pipeline.addTransform(std::move(merge));
    }

    pipeline.addSimpleTransform(
        [collector_ptr = collector,
         rate_function_ptr = rate_function,
         sum_function_ptr = sum_function,
         labels = labels_to_keep,
         max_samples = max_samples_per_series,
         max_groups = max_output_groups,
         output_block_size = max_output_block_size](const SharedHeader & header)
        {
            return std::make_shared<PromQLRangeSumByTransform>(
                header, collector_ptr, rate_function_ptr, sum_function_ptr, labels, max_samples, max_groups, output_block_size);
        });
}

void PromQLRangeSumByStep::updateOutputHeader()
{
    output_header = PromQLRangeSumByTransform::transformHeader(sum_function);
}

}
