#pragma once

#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Aggregates local hits and forwards aggregate argument columns with the block's recorded misses.
/// The producer waits for conversion and publication before reading memory and checking limits.
class AdaptiveAggregatingTransform final : public AggregatingTransformBase
{
public:
    AdaptiveAggregatingTransform(
        SharedHeader header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_, size_t current_variant);
    ~AdaptiveAggregatingTransform() override;

    String getName() const override { return "AdaptiveAggregatingTransform"; }
    Status prepare() override;
    void work() override;
    void onCancel() noexcept override;

private:
    /// The producer outlives the execution state that refers to it.
    std::unique_ptr<AdaptiveAggregationProducer> adaptive_context;
    std::unique_ptr<AdaptiveAggregationExecution> adaptive_execution;
    bool has_output_chunk = false;
    bool local_aggregation_finished = false;

    void finishAggregation();
};

}
