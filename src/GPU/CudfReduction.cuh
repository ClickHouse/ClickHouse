#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

/** A `sum`, `min` or `max` over a column with no `GROUP BY`, batch by batch on the device with
  * `cudf::reduce`. Each batch is reduced to one value, which is kept on the device beside the
  * others, and the values are reduced once at the end: nothing comes back to the host per batch.
  *
  * This header is what the host sees of the class; the state behind the pointer is the nvcc
  * island's, which alone includes cuDF.
  */
class CudfReduction
{
public:
    CudfReduction(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation);
    ~CudfReduction();

    CudfReduction(const CudfReduction &) = delete;
    CudfReduction & operator=(const CudfReduction &) = delete;

    void addBatch(DeviceColumnView values);

    /// The bits of the result over every batch so far, in `result_type`, after which the
    /// reduction starts over.
    uint64_t finalize();

private:
    struct State;
    State * state = nullptr;
};

}
