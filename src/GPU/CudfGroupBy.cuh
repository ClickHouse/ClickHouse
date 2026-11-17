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
    /// them, in its order as well. With `kept`, a mask on the device of a bit per row, set for the rows of the `WHERE`,
    /// in 32-bit words as `cudf::bitmask_type`, the other rows are left out; it stays as the columns do.
    virtual void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, const uint32_t * kept = nullptr) = 0;

    /// Returns the number of groups.
    virtual size_t finalize() = 0;

    /// After `finalize`: keeps the groups whose bit of `kept`, a mask as of `addBatch`, is set, as a `HAVING` does.
    /// Returns the number of groups kept. The views `key` and `value` gave before are no longer valid.
    virtual size_t keepGroups(const uint32_t * kept) = 0;

    virtual DeviceColumnView key(size_t index) const = 0;
    /// The result of aggregate `index`.
    virtual DeviceColumnView value(size_t index) const = 0;
};

/// The caller owns what it returns.
__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, cuda::stream_ref stream);

}
