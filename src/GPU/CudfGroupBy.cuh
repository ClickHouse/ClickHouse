#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// A device `GROUP BY` split by a hash of the keys into parts; of plain types, as both ClickHouse and the cuDF island include it.
class CudfGroupBy
{
public:
    /// Out of line, so that the vtable is emitted in the island alone.
    __host__ virtual ~CudfGroupBy();

    /// The columns must stay alive in the order of the stream; `kept` is a device bitmask of the rows to include.
    virtual void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, const uint32_t * kept = nullptr) = 0;

    /// Takes part `part` of `other`, which takes no more batches; several may absorb parts of one `other` at once.
    virtual void absorbPart(const CudfGroupBy & other, size_t part) = 0;

    virtual size_t finalize() = 0;

    virtual size_t keepGroups(const uint32_t * kept) = 0;

    virtual DeviceColumnView key(size_t index) const = 0;
    virtual DeviceColumnView value(size_t index) const = 0;
};

/// The caller owns the result.
__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys,
    GPUSpan<GPUElementType> values,
    GPUSpan<GPUGroupByAggregate> aggregates,
    size_t num_parts,
    cuda::stream_ref stream);

}
