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
    GPUSpan<uint32_t> columns;
};

/// A `GROUP BY` on the device: each batch is reduced into partial results (an `avg` as a `sum` and a `count`), which are
/// split by a hash of the keys into parts that others merge each on its own, see `absorbPart`. Without keys, all rows
/// are one group. The last `derived.size()` of `values` are computed on the device from the others.
class CudfGroupBy
{
public:
    __host__ CudfGroupBy(
        GPUSpan<GPUElementType> keys,
        GPUSpan<GPUElementType> values,
        GPUSpan<GPUDerivedValue> derived,
        GPUSpan<GPUGroupByAggregate> aggregates,
        size_t num_parts,
        cuda::stream_ref stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// Rows where `filter` is false are left out. `values` are without the derived ones. The columns must be ready and
    /// stay alive in the order of the stream.
    __host__ void addBatch(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> values,
        GPUSpan<DeviceFixedColumn> filter_columns = {},
        const GPUExpression * filter = nullptr);

    /// Of one of one part that groups no batch itself: takes part `part` of `other`, which takes no more batches.
    /// Several may take parts of one `other` at once.
    __host__ void absorbPart(const CudfGroupBy & other, size_t part);

    /// Combines the partial results of every part into the groups. Returns the number of groups.
    __host__ size_t finalize();

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

/// Collects the rows of `values` that a batch's `WHERE` keeps, as they are, rather than grouping them.
class CudfRowCollector
{
public:
    __host__ CudfRowCollector(GPUSpan<GPUElementType> values, cuda::stream_ref stream);
    __host__ ~CudfRowCollector();

    CudfRowCollector(const CudfRowCollector &) = delete;
    CudfRowCollector & operator=(const CudfRowCollector &) = delete;

    /// As `CudfGroupBy::addBatch`, of the values only.
    __host__ void addBatch(GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns = {}, const GPUExpression * filter = nullptr);

    /// How many rows are collected and not taken yet.
    __host__ size_t collectedRows() const;
    /// Makes the rows collected what `value` views, until the next call, and returns how many.
    __host__ size_t take();
    __host__ DeviceColumnView value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
