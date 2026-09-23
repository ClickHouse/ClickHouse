#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

/** A `GROUP BY` over integer keys kept on the device between batches: the keys packed into one
  * eight-byte word in a `cuco::static_set`, and the accumulators of a group's aggregates in one
  * record beside it. Each batch is folded into the groups there, and the host hears from it once,
  * at the end. A batch's `WHERE`, compiled into a small program, is evaluated in the grouping
  * kernel; a chunk with few groups next to its rows may be grouped in two passes, through
  * buckets in shared memory, when that is measured to be cheaper.
  *
  * This header is what the host sees of the class. The state behind the pointer is defined by
  * the nvcc island, which alone includes cuco and cuDF, so that both compilers agree on the
  * layout here.
  */
class RecordGroupBy
{
public:
    RecordGroupBy(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values);
    ~RecordGroupBy();

    RecordGroupBy(const RecordGroupBy &) = delete;
    RecordGroupBy & operator=(const RecordGroupBy &) = delete;

    /// Groups one batch and folds it into the groups kept on the device. Queued on the compute
    /// stream: the batch's buffers may be refilled through that stream at once, and must not be
    /// touched from the host until it has run. With a `filter`, only the rows it accepts,
    /// evaluated over `filter_columns`, are grouped. Answers how many microseconds the device
    /// spent in the grouping kernels, apart from whatever they waited for.
    double addBatch(
        GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter);

    /// Closes the groups to further batches and answers how many there are.
    size_t finalize();

    /// Copies the groups into host memory: a column per key, and a column per value in the type
    /// its `GPUGroupByValue` says it leaves a group in.
    void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values);

private:
    struct State;
    State * state = nullptr;
};

}
