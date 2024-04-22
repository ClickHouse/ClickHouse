#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Streaming/ConsumeOrderSequencer.h>
#include <Processors/Streaming/ReadingSourceOption.h>

namespace DB
{

class StreamingAdapterStep final : public IQueryPlanStep
{
public:
    explicit StreamingAdapterStep(
        DataStream storage_stream_,
        DataStream subscription_stream_,
        SequencerPtr sequencer_ = std::make_shared<ConsumeOrderSequencer>(),
        ReadingSourceOptions state_ = ReadingSourceOptions{ReadingSourceOption::Storage});

    ~StreamingAdapterStep() override = default;

    String getName() const override { return "StreamingAdapter"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    bool canUpdateInputStream() const override { return true; }

private:
    void updateOutputStream() override;

    Block storage_header;
    SequencerPtr sequencer;
    ReadingSourceOptions state;
};

}
