#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

namespace DB::GPU
{

/// Queues the computation of `derived` over the first `rows` rows of the value columns `values` into `out`, `rows`
/// doubles on the device.
__host__ void evaluateDerivedValue(
    const GPUDerivedValue & derived, GPUSpan<DeviceFixedColumn> values, size_t rows, double * out, cuda::stream_ref stream);

}
