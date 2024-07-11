#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class StreamingAdapterStep final : public IQueryPlanStep
{
public:
    explicit StreamingAdapterStep(DataStream subscription_stream_);
    StreamingAdapterStep(DataStream storage_stream_, DataStream subscription_stream_);

    ~StreamingAdapterStep() override = default;

    String getName() const override { return "StreamingAdapter"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    bool canUpdateInputStream() const override { return true; }

private:
    void updateOutputStream() override;

    Block output_header;
};

void makeStreamInfinite(QueryPlan & plan);

}
