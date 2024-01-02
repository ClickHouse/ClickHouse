#pragma once

#include <Processors/Transforms/PartialResultTransform.h>

namespace DB
{

class LimitTransform;

/// Currently support only single thread implementation with one input and one output ports
class LimitPartialResultTransform : public PartialResultTransform
{
public:
    using LimitTransformPtr = std::shared_ptr<LimitTransform>;

    LimitPartialResultTransform(
        const Block & header,
        UInt64 partial_result_limit_,
        UInt64 partial_result_duration_ms_,
        UInt64 limit_,
        UInt64 offset_);

    String getName() const override { return "LimitPartialResultTransform"; }

    void transformPartialResult(Chunk & chunk) override;
    /// LimitsTransform doesn't have a state which can be snapshoted
    ShaphotResult getRealProcessorSnapshot() override { return {{}, SnaphotStatus::Stopped}; }

private:
    UInt64 limit;
    UInt64 offset;

    LimitTransformPtr limit_transform;
};

}
