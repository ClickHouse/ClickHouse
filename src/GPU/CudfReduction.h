#pragma once

#include <GPU/IReduction.h>

#include <cudf/aggregation.hpp>
#include <cudf/types.hpp>

#include <rmm/device_buffer.hpp>

#include <memory>

namespace DB::GPU
{

/// A reduction in progress. Each batch is reduced to one value, which is copied onto the end of
/// `partials` within the device, and the partials are reduced once at the end: nothing comes back
/// to the host per batch, so the host stages the next batch while the device works on this one.
class CudfReduction final : public IReduction
{
public:
    CudfReduction(GPUElementType element_type_, GPUElementType result_type_, GPUAggregationKind aggregation_kind);

    void addBatch(DeviceColumnView values) override;

    uint64_t finalize() override;

private:
    const GPUElementType element_type;
    const GPUElementType result_type;

    /// What cuDF is asked to leave the result in: a `sum` accumulates in the wider result type,
    /// a `min` or `max` stays in the input's.
    const cudf::data_type output_type;

    const size_t output_size;

    const std::unique_ptr<cudf::reduce_aggregation> aggregation;

    rmm::device_buffer partials;
    size_t num_partials = 0;
};

}
