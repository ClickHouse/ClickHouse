#include <Processors/Merges/PromQLTwoRangeRatesMergingTransform.h>

#include <Common/Exception.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

const PromQLTwoRangeRatesFusionConfig & requireConfig(const PromQLTwoRangeRatesFusionConfigPtr & config)
{
    if (!config)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL fused two-rate merge configuration is null");
    return *config;
}

PromQLTwoRangeRatesSeriesMatcherPtr makeSeriesMatcher(
    const PromQLTwoRangeRatesFusionConfigPtr & config,
    PromQLTwoRangeRatesGroupStatePtr group_state)
{
    const auto & checked_config = requireConfig(config);
    if (!group_state)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL fused two-rate merge group state is null");

    return std::make_shared<PromQLTwoRangeRatesSeriesMatcher>(
        checked_config.collector,
        checked_config.first_metric_name,
        checked_config.second_metric_name,
        std::move(group_state));
}

}

PromQLTwoRangeRatesMergingTransform::PromQLTwoRangeRatesMergingTransform(
    SharedHeader input_header,
    size_t num_inputs,
    PromQLTwoRangeRatesFusionConfigPtr config,
    PromQLTwoRangeRatesGroupStatePtr group_state)
    : IMergingTransform(
          num_inputs,
          input_header,
          requireConfig(config).output_header,
          /*have_all_inputs_=*/true,
          /*limit_hint_=*/0,
          /*always_read_till_end_=*/false,
          input_header,
          num_inputs,
          requireConfig(config).collector,
          requireConfig(config).rate_function,
          requireConfig(config).max_samples_per_series,
          requireConfig(config).max_output_block_size,
          requireConfig(config).raw_min_time,
          requireConfig(config).raw_max_time,
          makeSeriesMatcher(config, std::move(group_state)))
{
}

}
