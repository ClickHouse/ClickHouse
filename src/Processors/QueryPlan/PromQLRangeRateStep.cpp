#include <Processors/QueryPlan/PromQLRangeRateStep.h>

#include <Processors/Merges/PromQLRangeRateMergingTransform.h>
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

PromQLRangeRateStep::PromQLRangeRateStep(
    SharedHeader input_header_,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    bool parallel_processing_requested_,
    size_t max_parallel_lanes_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_)
    : ITransformingStep(input_header_, PromQLRangeRateTransform::transformHeader(rate_function_), getTraits())
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_block_size(max_output_block_size_)
    , max_parallel_lanes(max_parallel_lanes_)
    , parallel_processing_requested(parallel_processing_requested_)
    , raw_min_time(std::move(raw_min_time_))
    , raw_max_time(std::move(raw_max_time_))
{
}

void PromQLRangeRateStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremes();

    if (pipeline.getNumStreams() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native range rate requires at least one ordered input stream");

    if (parallel_processing_enabled && pipeline.getNumStreams() > 1)
    {
        auto output_groups = std::make_shared<PromQLRangeRateGroupSet>();
        pipeline.addSimpleTransform(
            [collector_ptr = collector,
             rate_function_ptr = rate_function,
             max_samples = max_samples_per_series,
             output_block_size = max_output_block_size,
             output_groups,
             min_time = raw_min_time,
             max_time = raw_max_time](const SharedHeader & header)
            {
                return std::make_shared<PromQLRangeRateTransform>(
                    header, collector_ptr, rate_function_ptr, max_samples, output_block_size, output_groups, min_time, max_time);
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
                "PromQL native range rate requires {} and {} columns to merge {} ordered input streams",
                TimeSeriesColumnNames::ID,
                TimeSeriesColumnNames::Bucket,
                pipeline.getNumStreams());

        auto merge = std::make_shared<PromQLRangeRateMergingTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            collector,
            rate_function,
            max_samples_per_series,
            max_output_block_size,
            raw_min_time,
            raw_max_time);
        pipeline.addTransform(std::move(merge));
        return;
    }

    pipeline.addSimpleTransform(
        [collector_ptr = collector,
         rate_function_ptr = rate_function,
         max_samples = max_samples_per_series,
         output_block_size = max_output_block_size,
         min_time = raw_min_time,
         max_time = raw_max_time](const SharedHeader & header)
        {
            return std::make_shared<PromQLRangeRateTransform>(
                header, collector_ptr, rate_function_ptr, max_samples, output_block_size, nullptr, min_time, max_time);
        });
}

void PromQLRangeRateStep::updateOutputHeader()
{
    output_header = PromQLRangeRateTransform::transformHeader(rate_function);
}

}
