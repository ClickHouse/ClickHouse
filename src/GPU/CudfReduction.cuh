#pragma once

#include <GPU/GPUTypes.cuh>

namespace DB::GPU
{

class CudfReduction
{
public:
    CudfReduction(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation);
    ~CudfReduction();

    CudfReduction(const CudfReduction &) = delete;
    CudfReduction & operator=(const CudfReduction &) = delete;

    void addBatch(DeviceFixedColumn values);

    uint64_t finalize();

private:
    struct State;
    State * state = nullptr;
};

}
