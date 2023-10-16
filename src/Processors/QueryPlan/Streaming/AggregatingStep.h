#pragma once

#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <vector>

namespace DB
{

namespace Streaming
{
struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// Streaming Aggregation. See StreamingAggregatingTransform.
class AggregatingStep : public ITransformingStep
{
public:
    AggregatingStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool emit_version_);

    static Block appendGroupingColumn(Block block, const Names & keys, bool has_grouping, bool use_nulls);

    String getName() const override { return "StreamingAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }

private:
    void updateOutputStream() override;

    Aggregator::Params params;
    bool final;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    bool emit_version;

    Processors aggregating;
};
}
}
