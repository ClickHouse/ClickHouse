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
class RowFilter;
}

/// A device `GROUP BY` split by a hash of the keys into parts; of plain types, as both ClickHouse and the cuDF island include it.
class CudfGroupBy
{
public:
    __host__ CudfGroupBy(
        GPUSpan<GPUElementType> keys,
        GPUSpan<GPUElementType> values,
        GPUSpan<GPUGroupByAggregate> aggregates,
        size_t num_parts,
        cuda::stream_ref stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// Leaves out the rows where `filter` is false. The columns must stay alive in the order of the stream.
    __host__ void addBatch(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> values,
        GPUSpan<DeviceFixedColumn> filter_columns = {},
        const GPUExpression * filter = nullptr);

    /// Takes part `part` of `other`, which takes no more batches; several may absorb parts of one `other` at once.
    __host__ void absorbPart(const CudfGroupBy & other, size_t part);

    __host__ size_t finalize();

    /// `columns[i]` is what `having` reads as its column `i`: a key, or an aggregate after the keys.
    __host__ size_t filterGroups(const GPUExpression & having, GPUSpan<size_t> columns);

    __host__ DeviceColumnView key(size_t index) const;
    __host__ DeviceColumnView value(size_t index) const;

private:
    cudf_group_by::GroupBy * impl = nullptr;
};

/// The rows of a batch that `where`, a predicate, keeps, rather than groups of them. `where` points into the
/// `CompiledGPUExpression` that owns its kernel, which this side of the libraries cannot hold: it must outlive the filter.
class CudfRowFilter
{
public:
    __host__ CudfRowFilter(GPUSpan<GPUElementType> values, const GPUExpression & where, cuda::stream_ref stream);
    __host__ ~CudfRowFilter();

    CudfRowFilter(const CudfRowFilter &) = delete;
    CudfRowFilter & operator=(const CudfRowFilter &) = delete;

    /// Returns how many rows `where` keeps, which `value` views until the next call: the columns of the batch themselves
    /// if it keeps them all, which must stay alive until then.
    __host__ size_t filter(GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> where_columns);

    __host__ DeviceColumnView value(size_t index) const;

private:
    cudf_group_by::RowFilter * impl = nullptr;
};

}
