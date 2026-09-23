#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

class IReduction
{
public:
    virtual ~IReduction() = default;

    /// Folds one batch into the partial result kept on the device. Queued on the device's stream:
    /// the batch's buffer may be refilled through that stream at once, and must not be touched from
    /// the host until it has run.
    virtual void addBatch(DeviceColumnView values) = 0;

    /// Answers the result over every batch added so far, widened to eight bytes - a `Float64` comes
    /// back as its bits, so that every result type crosses the same way - and starts over.
    virtual uint64_t finalize() = 0;

    static IReduction * create(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation);
};

}
