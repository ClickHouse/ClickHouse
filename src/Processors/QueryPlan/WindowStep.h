#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

class WindowTransform;

class WindowStep : public ITransformingStep
{
public:
    explicit WindowStep(const Header & input_header_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> & window_functions_,
            bool streams_fan_out_);

    String getName() const override { return "Window"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const WindowDescription & getWindowDescription() const;

private:
    void updateOutputHeader() override;

    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
    bool streams_fan_out;
};

}
