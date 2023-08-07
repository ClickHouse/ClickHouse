#pragma once

#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/PartialResultTransform.h>

namespace DB
{

class AggregatingPartialResultTransform : public PartialResultTransform
{
public:
    using AggregatingTransformPtr = std::shared_ptr<AggregatingTransform>;

    AggregatingPartialResultTransform(
        const Block & input_header, const Block & output_header, AggregatingTransformPtr aggregating_transform_,
        UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_);

    String getName() const override { return "AggregatingPartialResultTransform"; }

    ShaphotResult getRealProcessorSnapshot() override;

private:
    AggregatingTransformPtr aggregating_transform;
};

}
