#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

class CudfGroupBy
{
public:
    __host__ CudfGroupBy(GPUSpan<GPUElementType> keys, GPUSpan<GPUGroupByValue> values, rmm::cuda_stream_view stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    __host__ void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values);

    __host__ size_t finalize();

    __host__ DeviceColumnView key(size_t index) const;
    __host__ DeviceColumnView value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
