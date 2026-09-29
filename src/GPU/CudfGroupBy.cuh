#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/** A `GROUP BY` with `cudf::groupby`, for keys that `RecordGroupBy` cannot pack into one word:
  * strings, alongside fixed-width integers. The values are integers, reduced by `sum`, `min` or
  * `max`.
  *
  * Each batch is grouped on its own, and the groups of the batches are merged by grouping them
  * again - the sums of the partial sums, the minimums of the minimums - whenever the batches
  * gathered since the last merge have more groups than the merge left, so that a key seen in many
  * batches is merged a logarithmic number of times.
  *
  * Everything runs on a stream the caller owns, which must not be the legacy default stream:
  * cuco copies a count back with `cudaMemcpyBatchAsync`, which refuses it. The caller orders the
  * stream after what filled a batch, and what refills it after the stream.
  *
  * This header is what the host sees of the class; the state behind the pointer is the nvcc
  * island's, which alone includes cuDF.
  */
class CudfGroupBy
{
public:
    CudfGroupBy(GPUSpan<GPUElementType> keys, GPUSpan<GPUGroupByValue> values, rmm::cuda_stream_view stream);
    ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// Groups one batch and folds it into the groups so far, reading the batch on the stream.
    void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceFixedColumn> values);

    /// Closes the groups to further batches and answers how many there are.
    size_t finalize();

    /// The groups, on the device until the object goes: a column per key, of the key's type, and a
    /// column per value in the type its `GPUGroupByValue` says it leaves a group in.
    DeviceColumnView key(size_t index) const;
    DeviceFixedColumn value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

/// Writes each of `count` offsets at `from` less `minus` to `to`, which may be `from`, on `stream`:
/// what a column of strings on the device does to its offsets when the bytes before its first row
/// are dropped, or when some of its rows are viewed as a column of their own, from 0.
void subtractFromOffsets(const uint64_t * from, size_t count, uint64_t minus, uint64_t * to, rmm::cuda_stream_view stream);

}
