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
/// Without keys and aggregates, it collects the rows of `values` that a batch's `WHERE` keeps instead, as they are, and
/// `value` is of its collected columns.
///
/// The last `derived.size()` of `values` are not in the batches: they are computed on the device from the others, the
/// `Float64` values of each `GPUDerivedValue`, over the rows of a batch that its `WHERE` keeps.
class CudfGroupBy
{
public:
    __host__ CudfGroupBy(
        GPUSpan<GPUElementType> keys,
        GPUSpan<GPUElementType> values,
        GPUSpan<GPUDerivedValue> derived,
        GPUSpan<GPUGroupByAggregate> aggregates,
        cuda::stream_ref stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// The rows for which `filter`, when given, is false over `filter_columns` are left out. Of `values`, those of the
    /// batch, without the derived ones. The columns must be there in the order of the stream of the `CudfGroupBy`, and
    /// must stay until it has grouped them, in its order as well.
    __host__ void addBatch(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> values,
        GPUSpan<DeviceFixedColumn> filter_columns = {},
        const GPUFilterProgram * filter = nullptr);

    __host__ size_t finalize();

    /// After `finalize`: keeps the groups for which `program` holds over the keys and the aggregates' results, each of a
    /// fixed width, that `columns` names, the keys first and then the aggregates. Returns the number of groups kept.
    __host__ size_t filterGroups(const GPUFilterProgram & program, GPUSpan<uint32_t> columns);

    __host__ DeviceColumnView key(size_t index) const;
    /// The result of aggregate `index`.
    __host__ DeviceColumnView value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
