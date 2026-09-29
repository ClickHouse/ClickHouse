#pragma once

#include <GPU/GPUTypes.cuh>

namespace DB::GPU
{

class RecordGroupBy
{
public:
    RecordGroupBy(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values);
    ~RecordGroupBy();

    RecordGroupBy(const RecordGroupBy &) = delete;
    RecordGroupBy & operator=(const RecordGroupBy &) = delete;

    double addBatch(
        GPUSpan<DeviceFixedColumn> keys, GPUSpan<DeviceFixedColumn> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUFilterProgram * filter);

    size_t finalize();

    void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values);

private:
    struct State;
    State * state = nullptr;
};

}
