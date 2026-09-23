#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

class IGroupBy
{
public:
    virtual ~IGroupBy() = default;

    virtual double addBatch(
        GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter)
        = 0;

    virtual size_t finalize() = 0;

    virtual void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values) = 0;

    static IGroupBy * create(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values);
};

}
