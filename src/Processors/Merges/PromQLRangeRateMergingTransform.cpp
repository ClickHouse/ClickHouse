#include <Processors/Merges/PromQLRangeRateMergingTransform.h>

#include <utility>


namespace DB
{

PromQLRangeRateMergingTransform::PromQLRangeRateMergingTransform(
    SharedHeader input_header,
    size_t num_inputs,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_)
    : IMergingTransform(
          num_inputs,
          input_header,
          PromQLRangeRateTransform::transformHeader(rate_function_),
          /*have_all_inputs_=*/true,
          /*limit_hint_=*/0,
          /*always_read_till_end_=*/false,
          input_header,
          num_inputs,
          std::move(collector_),
          std::move(rate_function_),
          max_samples_per_series_,
          max_output_block_size_,
          std::move(raw_min_time_),
          std::move(raw_max_time_))
{
}

}
