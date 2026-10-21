#pragma once

#include <Processors/Merges/Algorithms/PromQLRangeRateMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>


namespace DB
{

/// Ordered multi-input merge which evaluates the exact native two-rate island
/// without materializing a merged `samples` array.
class PromQLTwoRangeRatesMergingTransform final : public IMergingTransform<PromQLRangeRateMergingAlgorithm>
{
public:
    PromQLTwoRangeRatesMergingTransform(
        SharedHeader input_header,
        size_t num_inputs,
        PromQLTwoRangeRatesFusionConfigPtr config,
        PromQLTwoRangeRatesGroupStatePtr group_state);

    String getName() const override { return "PromQLTwoRangeRatesMergingTransform"; }
};

}
