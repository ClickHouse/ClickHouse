#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class WindowTransform;

class WindowStep : public ITransformingStep
{
public:
    explicit WindowStep(const DataStream & input_stream_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> & window_functions_);

    String getName() const override { return "Window"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const WindowDescription & getWindowDescription() const;

private:
    void updateOutputStream() override;

    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
};

}
