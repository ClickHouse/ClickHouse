#pragma once

#include <GPU/GPUExpression.cuh>
#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

namespace cudf_group_by
{
class GroupBy;
class RowCollector;
}

/// An argument of an aggregate that is an expression over the values of a batch rather than one of them, such as
/// `l_extendedprice * (1 - l_discount)` of `sum(l_extendedprice * (1 - l_discount))`. The device computes it for every
/// batch by the compiled `expression`, so that only the values it reads go to the device. Of a fixed width: `columns[i]`
/// is the value of the batch that the expression reads as its column `i`.
struct GPUComputedArgument
{
    GPUExpression expression;
    GPUSpan<uint32_t> columns;
};

/// A device `GROUP BY` split by a hash of the keys into parts; of plain types, as both ClickHouse and the cuDF island include it.
class CudfGroupBy
{
public:
    /// The aggregates take the `values` of a batch, then the `computed` values, which the device computes for each batch.
    __host__ CudfGroupBy(
        GPUSpan<GPUElementType> keys,
        GPUSpan<GPUElementType> values,
        GPUSpan<GPUComputedArgument> computed,
        GPUSpan<GPUGroupByAggregate> aggregates,
        size_t num_parts,
        cuda::stream_ref stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// Without the computed arguments. The rows where `filter`, a predicate over `filter_columns`, is false are left out. The
    /// columns must stay alive in the order of the stream.
    __host__ void addBatch(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> values,
        GPUSpan<DeviceFixedColumn> filter_columns = {},
        const GPUExpression * filter = nullptr);

    /// Takes part `part` of `other`, which takes no more batches; several may absorb parts of one `other` at once.
    __host__ void absorbPart(const CudfGroupBy & other, size_t part);

    __host__ size_t finalize();

    /// Keeps the groups where `having`, a predicate, holds. `columns[i]` is the group column it reads as its column `i`:
    /// a key index, or an aggregate index after the keys.
    __host__ size_t filterGroups(const GPUExpression & having, GPUSpan<size_t> columns);

    __host__ DeviceColumnView key(size_t index) const;
    __host__ DeviceColumnView value(size_t index) const;

private:
    cudf_group_by::GroupBy * impl = nullptr;
};

/// Collects the rows of a batch that its `WHERE` keeps, as they are, rather than grouping them.
class CudfRowCollector
{
public:
    __host__ CudfRowCollector(GPUSpan<GPUElementType> values, cuda::stream_ref stream);
    __host__ ~CudfRowCollector();

    CudfRowCollector(const CudfRowCollector &) = delete;
    CudfRowCollector & operator=(const CudfRowCollector &) = delete;

    /// As `CudfGroupBy::addBatch`, of the values only.
    __host__ void addBatch(GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns = {}, const GPUExpression * filter = nullptr);

    /// The rows collected and not taken yet.
    __host__ size_t collectedRows() const;

    /// Makes the rows collected so far what `value` views, until the next call, and returns how many there are.
    __host__ size_t take();

    __host__ DeviceColumnView value(size_t index) const;

private:
    cudf_group_by::RowCollector * impl = nullptr;
};

}
