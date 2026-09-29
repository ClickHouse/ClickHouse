#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

class CudfHashJoin
{
public:
    CudfHashJoin(GPUElementType key_element_type, GPUSpan<GPUElementType> payload_element_types);
    ~CudfHashJoin();

    CudfHashJoin(const CudfHashJoin &) = delete;
    CudfHashJoin & operator=(const CudfHashJoin &) = delete;

    void build(DeviceFixedColumn keys, GPUSpan<DeviceFixedColumn> payloads);

private:
    friend class CudfHashJoinProbe;

    struct State;
    State * state = nullptr;
};

class CudfHashJoinProbe
{
public:
    CudfHashJoinProbe(const CudfHashJoin & join, rmm::cuda_stream_view stream);
    ~CudfHashJoinProbe();

    CudfHashJoinProbe(const CudfHashJoinProbe &) = delete;
    CudfHashJoinProbe & operator=(const CudfHashJoinProbe &) = delete;

    size_t probe(DeviceFixedColumn keys);

    DeviceFixedColumn probeRowIndices() const;
    DeviceFixedColumn gatheredPayload(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
