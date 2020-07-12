#pragma once
#include <DataStreams/SizeLimits.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>

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
            bool pre_distinct_,
            InputOrderInfoPtr distinct_info_); /// If is enabled, execute distinct for separate streams. Otherwise, merge streams.

    String getName() const override { return "Distinct"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    SizeLimits set_size_limits;
    UInt64 limit_hint;
    Names columns;
    bool pre_distinct;

    InputOrderInfoPtr distinct_info;
};

}
