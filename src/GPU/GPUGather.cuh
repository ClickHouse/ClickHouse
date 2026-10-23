#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

namespace DB::GPU
{

/// Queues on `stream` the gather of `rows` values of `element_size` bytes each: value `i` of `out` is value `indices[i]`
/// of `values`. All three are on the device.
__host__ void gatherFixedRows(
    const char * values, size_t element_size, const int32_t * indices, size_t rows, char * out, cuda::stream_ref stream);

}
