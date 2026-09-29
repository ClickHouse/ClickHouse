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

    void addBatch(DeviceColumnView values);

    /// The reduction of every batch so far, as a column of one row on the device that lives as long as this does, or
    /// of no rows where there was no batch.
    DeviceColumnView finalize();

private:
    struct State;
    State * state = nullptr;
};

}
