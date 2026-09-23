#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

class IReduction
{
public:
    virtual ~IReduction() = default;

    virtual void addBatch(DeviceColumnView values) = 0;

    virtual uint64_t finalize() = 0;

    static IReduction * create(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation);
};

}
