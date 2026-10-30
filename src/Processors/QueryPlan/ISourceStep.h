#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <functional>

namespace DB
{

/// Step which takes empty pipeline and initializes it. Returns single logical DataStream.
class ISourceStep : public IQueryPlanStep
{
public:
    explicit ISourceStep(SharedHeader output_header_);

    ISourceStep(const ISourceStep &) = default;
    ISourceStep(ISourceStep &&) = default;

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    virtual void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) = 0;

    void describePipeline(FormatSettings & settings) const override;

    bool hasCorrelatedExpressions() const override { return false; }

    /// Called when the pipeline is built. A step that replaces a whole read reports the index usage of that read with it.
    void setOnInitializePipeline(std::function<void()> callback) { on_initialize_pipeline = std::move(callback); }

protected:
    void updateOutputHeader() override {}

private:
    std::function<void()> on_initialize_pipeline;
};

}
