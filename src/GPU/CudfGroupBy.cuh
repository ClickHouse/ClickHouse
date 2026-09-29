#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

class CudfGroupBy
{
public:
    CudfGroupBy(GPUSpan<GPUElementType> keys, GPUSpan<GPUGroupByValue> values, rmm::cuda_stream_view stream);
    ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceFixedColumn> values);

    size_t finalize();

    DeviceColumnView key(size_t index) const;
    DeviceFixedColumn value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

void subtractFromOffsets(const uint64_t * from, size_t count, uint64_t minus, uint64_t * to, rmm::cuda_stream_view stream);

}
