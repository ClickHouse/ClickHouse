#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class StreamingAdapterStep final : public IQueryPlanStep
{
public:
    explicit StreamingAdapterStep(DataStream subscription_stream_);
    explicit StreamingAdapterStep(DataStream storage_stream_, DataStream subscription_stream_);

    ~StreamingAdapterStep() override = default;

    String getName() const override { return "StreamingAdapter"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    bool canUpdateInputStream() const override { return true; }

private:
    void updateOutputStream() override;

    size_t input_streams_count;
    Block output_header;
};

void makeStreamInfinite(QueryPlan & plan);

}
