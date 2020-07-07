#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
            const DataStream & input_stream_,
            const SizeLimits & set_size_limits_,
            UInt64 limit_hint_,
            const Names & columns_,
            bool pre_distinct_); /// If is enabled, execute distinct for separate streams. Otherwise, merge streams.

    String getName() const override { return "Distinct"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    SizeLimits set_size_limits;
    UInt64 limit_hint;
    Names columns;
    bool pre_distinct;
};

}
