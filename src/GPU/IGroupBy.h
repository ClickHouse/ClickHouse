#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

class IGroupBy
{
public:
    virtual ~IGroupBy() = default;

    /// Groups one batch and folds it into the partial result kept on the device. Queued on the
    /// device's stream: the batch's buffers may be refilled through that stream at once, and must
    /// not be touched from the host until it has run. With a `filter`, only the rows it accepts,
    /// evaluated over `filter_columns`, are grouped. Answers how many microseconds the device spent
    /// in the grouping kernels themselves, apart from whatever they waited for.
    virtual double addBatch(
        GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter)
        = 0;

    /// Closes the partial result to further batches and answers how many groups it holds.
    virtual size_t finalize() = 0;

    /// Copies the groups into host memory: a column per key, and a column per value in the type
    /// its `GPUGroupByValue` says it leaves a group in.
    virtual void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values) = 0;

    static IGroupBy * create(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values);
};

}
