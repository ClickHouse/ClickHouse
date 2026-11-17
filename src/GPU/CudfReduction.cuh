#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

namespace DB::GPU
{

/// A `sum`, a `min` or a `max` of the values of every batch, reduced in `stream`.
class CudfReduction
{
public:
    __host__ CudfReduction(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation, cuda::stream_ref stream);
    __host__ ~CudfReduction();

    CudfReduction(const CudfReduction &) = delete;
    CudfReduction & operator=(const CudfReduction &) = delete;

    __host__ void addBatch(DeviceColumnView values);

    /// The reduction of every batch so far, as a column of one row on the device that lives as long as this does, or
    /// of no rows where there was no batch.
    __host__ DeviceColumnView finalize();

private:
    struct State;
    State * state = nullptr;
};

}
