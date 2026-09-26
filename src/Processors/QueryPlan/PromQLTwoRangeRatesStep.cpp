#include <Processors/QueryPlan/PromQLTwoRangeRatesStep.h>

#include <Processors/Merges/MergingSortedTransform.h>
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

PromQLTwoRangeRatesStep::PromQLTwoRangeRatesStep(
    SharedHeader input_header_,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    String first_metric_name_,
    String second_metric_name_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    size_t max_join_groups_,
    size_t max_grid_cells_,
    bool parallel_processing_requested_,
    size_t max_parallel_lanes_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_,
    bool storage_fusion_requested_)
    : ITransformingStep(input_header_, PromQLTwoRangeRatesTransform::transformHeader(rate_function_), getTraits())
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , first_metric_name(std::move(first_metric_name_))
    , second_metric_name(std::move(second_metric_name_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_block_size(max_output_block_size_)
    , max_join_groups(max_join_groups_)
    , max_grid_cells(max_grid_cells_)
    , max_parallel_lanes(max_parallel_lanes_)
    , parallel_processing_requested(parallel_processing_requested_)
    , raw_min_time(std::move(raw_min_time_))
    , raw_max_time(std::move(raw_max_time_))
    , storage_fusion_requested(storage_fusion_requested_)
{
}

void PromQLTwoRangeRatesStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremes();

    if (pipeline.getNumStreams() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native two-rate island requires at least one ordered input stream");

    if (parallel_processing_enabled && pipeline.getNumStreams() > 1)
    {
        auto group_state = std::make_shared<PromQLTwoRangeRatesGroupState>(max_join_groups, max_grid_cells);
        pipeline.addSimpleTransform(
            [collector_ptr = collector,
             rate_function_ptr = rate_function,
             first_metric = first_metric_name,
             second_metric = second_metric_name,
             max_samples = max_samples_per_series,
             output_block_size = max_output_block_size,
             max_groups = max_join_groups,
             grid_cells = max_grid_cells,
             group_state,
             min_time = raw_min_time,
             max_time = raw_max_time](const SharedHeader & header)
            {
                return std::make_shared<PromQLTwoRangeRatesTransform>(
                    header,
                    collector_ptr,
                    rate_function_ptr,
                    first_metric,
                    second_metric,
                    max_samples,
                    output_block_size,
                    max_groups,
                    grid_cells,
                    group_state,
                    min_time,
                    max_time);
            });
        pipeline.resize(1);
        return;
    }

    if (pipeline.getNumStreams() > 1)
    {
        const auto & header = *pipeline.getSharedHeader();
        if (!header.has(TimeSeriesColumnNames::ID) || !header.has(TimeSeriesColumnNames::Bucket))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PromQL native two-rate island requires {} and {} columns to merge {} ordered input streams",
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
         first_metric = first_metric_name,
         second_metric = second_metric_name,
         max_samples = max_samples_per_series,
         output_block_size = max_output_block_size,
         max_groups = max_join_groups,
         grid_cells = max_grid_cells,
         min_time = raw_min_time,
         max_time = raw_max_time](const SharedHeader & header)
        {
            return std::make_shared<PromQLTwoRangeRatesTransform>(
                header,
                collector_ptr,
                rate_function_ptr,
                first_metric,
                second_metric,
                max_samples,
                output_block_size,
                max_groups,
                grid_cells,
                nullptr,
                min_time,
                max_time);
        });
}

void PromQLTwoRangeRatesStep::updateOutputHeader()
{
    output_header = PromQLTwoRangeRatesTransform::transformHeader(rate_function);
}

PromQLTwoRangeRatesFusionConfigPtr PromQLTwoRangeRatesStep::getFusionConfig() const
{
    return std::make_shared<const PromQLTwoRangeRatesFusionConfig>(
        collector,
        rate_function,
        first_metric_name,
        second_metric_name,
        max_samples_per_series,
        max_output_block_size,
        max_join_groups,
        max_grid_cells,
        raw_min_time,
        raw_max_time,
        getOutputHeader());
}

}
