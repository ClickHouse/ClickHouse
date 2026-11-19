#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

namespace cudf_group_by
{
class GroupBy;
}

/// A device `GROUP BY` split by a hash of the keys into parts; of plain types, as both ClickHouse and the cuDF island include it.
class CudfGroupBy
{
public:
    __host__ CudfGroupBy(
        GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, size_t num_parts, cuda::stream_ref stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// The columns must stay alive in the order of the stream.
    __host__ void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, DeviceBitmask kept = {});

    /// Takes part `part` of `other`, which takes no more batches; several may absorb parts of one `other` at once.
    __host__ void absorbPart(const CudfGroupBy & other, size_t part);

    __host__ size_t finalize();

    __host__ size_t keepGroups(DeviceBitmask kept);

    __host__ DeviceColumnView key(size_t index) const;
    __host__ DeviceColumnView value(size_t index) const;

private:
    cudf_group_by::GroupBy * impl = nullptr;
};

}
