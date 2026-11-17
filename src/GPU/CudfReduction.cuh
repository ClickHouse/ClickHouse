#pragma once

#include <GPU/GPUTypes.cuh>

namespace DB::GPU
{

class CudfReduction
{
public:
    __host__ CudfReduction(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation);
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
