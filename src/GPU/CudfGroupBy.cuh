#pragma once

#include <GPU/GPUExpression.cuh>
#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// A value column of a `GROUP BY` that the device computes from other value columns by `expression`, into a `Float64`:
/// the `columns[i]`-th value column is what it reads as its column `i`.
struct GPUDerivedValue
{
    GPUExpression expression;
    uint32_t columns[max_expression_columns];
    uint32_t num_columns = 0;
};

/// A `GROUP BY` on the device through one `cudf::groupby::streaming_groupby`, which keeps the groups on the device across
/// the batches: of keys of integers and strings, by the aggregates of value columns, several of one column or a `count` of
/// none among them.
///
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
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
        const GPUExpression * filter = nullptr);

    __host__ size_t finalize();

    /// Of rows collected rather than grouped: how many are collected and not taken yet, and makes them what `value`
    /// views, until the next call, returning how many. Before `finalize` as well.
    __host__ size_t collectedRows() const;
    __host__ size_t takeCollected();

    /// After `finalize`: keeps the groups for which `expression` holds over the keys and the aggregates' results, each of a
    /// fixed width, that `columns` names, the keys first and then the aggregates. Returns the number of groups kept.
    __host__ size_t filterGroups(const GPUExpression & expression, GPUSpan<uint32_t> columns);

    __host__ DeviceColumnView key(size_t index) const;
    /// The result of aggregate `index`.
    __host__ DeviceColumnView value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
