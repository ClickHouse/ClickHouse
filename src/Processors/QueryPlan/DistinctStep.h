#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
        const DataStream & input_stream_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool pre_distinct_, /// If is enabled, execute distinct for separate streams. Otherwise, merge streams.
        bool optimize_distinct_in_order_);

    String getName() const override { return "Distinct"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    SizeLimits set_size_limits;
    UInt64 limit_hint;
    Names columns;
    bool pre_distinct;
    bool optimize_distinct_in_order;
};

}
