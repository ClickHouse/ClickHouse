#pragma once

#include <GPU/GPUTypes.cuh>

namespace DB::GPU
{

class RecordGroupBy
{
public:
    __host__ RecordGroupBy(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values);
    __host__ ~RecordGroupBy();

    RecordGroupBy(const RecordGroupBy &) = delete;
    RecordGroupBy & operator=(const RecordGroupBy &) = delete;

    __host__ double addBatch(
        GPUSpan<DeviceFixedColumn> keys, GPUSpan<DeviceFixedColumn> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUFilterProgram * filter);

    __host__ size_t finalize();

    __host__ void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values);

private:
    struct State;
    State * state = nullptr;
};

}
