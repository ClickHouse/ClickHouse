#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// A `GROUP BY` on the device: each batch is reduced into partial results (an `avg` as a `sum` and a `count`), which are
/// split by a hash of the keys into parts that others merge each on its own, see `absorbPart`. Without keys, all rows
/// are one group.
///
/// An interface of plain types: both ClickHouse and the GPU island of cuDF, of another standard library, include it.
class CudfGroupBy
{
public:
    /// Out of line, so that the vtable is emitted in the island alone, where the implementation is.
    __host__ virtual ~CudfGroupBy();

    /// The columns must be ready and stay alive in the order of the stream. With `kept`, a bit per row on the device
    /// in 32-bit words, rows whose bit is not set are left out.
    virtual void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, const uint32_t * kept = nullptr) = 0;

    /// Of one of one part that groups no batch itself: takes part `part` of `other`, which takes no more batches.
    /// Several may take parts of one `other` at once.
    virtual void absorbPart(const CudfGroupBy & other, size_t part) = 0;

    /// Combines the partial results of every part into the groups. Returns the number of groups.
    virtual size_t finalize() = 0;

    /// After `finalize`: keeps the groups whose bit of `kept` is set. Returns the number kept.
    virtual size_t keepGroups(const uint32_t * kept) = 0;

    virtual DeviceColumnView key(size_t index) const = 0;
    /// The result of aggregate `index`.
    virtual DeviceColumnView value(size_t index) const = 0;
};

/// The caller owns what it returns.
__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys,
    GPUSpan<GPUElementType> values,
    GPUSpan<GPUGroupByAggregate> aggregates,
    size_t num_parts,
    cuda::stream_ref stream);

}
