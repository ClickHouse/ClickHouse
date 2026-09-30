#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

class CudfHashJoin
{
public:
    __host__ CudfHashJoin(GPUElementType key_element_type, GPUSpan<GPUElementType> payload_element_types);
    __host__ ~CudfHashJoin();

    CudfHashJoin(const CudfHashJoin &) = delete;
    CudfHashJoin & operator=(const CudfHashJoin &) = delete;

    __host__ void build(DeviceFixedColumn keys, GPUSpan<DeviceFixedColumn> payloads);

private:
    friend class CudfHashJoinProbe;

    struct State;
    State * state = nullptr;
};

class CudfHashJoinProbe
{
public:
    __host__ CudfHashJoinProbe(const CudfHashJoin & join, rmm::cuda_stream_view stream);
    __host__ ~CudfHashJoinProbe();

    CudfHashJoinProbe(const CudfHashJoinProbe &) = delete;
    CudfHashJoinProbe & operator=(const CudfHashJoinProbe &) = delete;

    __host__ size_t probe(DeviceFixedColumn keys);

    __host__ DeviceFixedColumn probeRowIndices() const;
    __host__ DeviceFixedColumn gatheredPayload(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
