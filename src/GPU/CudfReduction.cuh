#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

namespace DB::GPU
{

class CudfReduction
{
public:
    __host__ CudfReduction(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation, cuda::stream_ref stream);
    __host__ ~CudfReduction();

    CudfReduction(const CudfReduction &) = delete;
    CudfReduction & operator=(const CudfReduction &) = delete;

    __host__ void addBatch(DeviceColumnView values);

    /// Lives as long as this; has no rows if there was no batch.
    __host__ DeviceColumnView finalize();

private:
    struct State;
    State * state = nullptr;
};

}
