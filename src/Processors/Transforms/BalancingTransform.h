#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/InsertMemoryThrottle.h>

namespace DB
{

/**
 * BalancingTransform implements backpressure based on memory pressure
 *
 * When InsertMemoryThrottle reports we're throttled (memory usage > high_threshold),
 * this transform returns PortFull to create backpressure, reducing the number of active threads in the pipeline
 *
 * When memory drops below low_threshold, normal processing resumes
 */
class BalancingTransform : public ISimpleTransform
{
public:
    BalancingTransform(
        SharedHeader header_,
        InsertMemoryThrottlePtr throttle_);

    String getName() const override { return "BalancingTransform"; }

    Status prepare() override;
    void transform(Chunk & chunk) override;

private:
    InsertMemoryThrottlePtr throttle;
};

}
