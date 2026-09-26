#pragma once

#include <Processors/Merges/Algorithms/PromQLRangeRateMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>


namespace DB
{

/// Implementation of `PromQLRangeRateMergingAlgorithm` as a multi-input
/// pipeline processor.
class PromQLRangeRateMergingTransform final : public IMergingTransform<PromQLRangeRateMergingAlgorithm>
{
public:
    using CollectorPtr = PromQLRangeRateMergingAlgorithm::CollectorPtr;

    PromQLRangeRateMergingTransform(
        SharedHeader input_header,
        size_t num_inputs,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        std::optional<Field> raw_min_time_ = {},
        std::optional<Field> raw_max_time_ = {});

    String getName() const override { return "PromQLRangeRateMergingTransform"; }
};

}
