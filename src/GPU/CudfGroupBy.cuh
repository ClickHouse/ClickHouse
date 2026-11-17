#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// A `GROUP BY` on the device through one `cudf::groupby::streaming_groupby`, which keeps the groups on the device across
/// the batches: of keys of integers and strings, by the aggregates of value columns, several of one column or a `count` of
/// none among them.
///
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
///
/// An interface of plain types: both ClickHouse and the GPU island of cuDF, of another standard library, include it, and
/// the implementation, of the types of cuDF, is in the island alone.
class CudfGroupBy
{
public:
    /// Out of line, so that the vtable is emitted in the island alone, where the implementation is.
    __host__ virtual ~CudfGroupBy();

    /// The columns must be there in the order of the stream of the `CudfGroupBy`, and must stay until it has grouped
    /// them, in its order as well.
    virtual void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values) = 0;

    /// Returns the number of groups.
    virtual size_t finalize() = 0;

    virtual DeviceColumnView key(size_t index) const = 0;
    /// The result of aggregate `index`.
    virtual DeviceColumnView value(size_t index) const = 0;
};

/// The caller owns what it returns.
__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, cuda::stream_ref stream);

}
