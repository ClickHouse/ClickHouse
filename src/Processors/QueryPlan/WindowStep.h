#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Interpreters/WindowDescription.h>
#include <optional>

namespace DB
{

class WindowTransform;

class WindowStep : public ITransformingStep
{
public:
    explicit WindowStep(const SharedHeader & input_header_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> & window_functions_,
            bool streams_fan_out_);

    String getName() const override { return "Window"; }

    bool hasCorrelatedExpressions() const override
    {
        for (const auto & actions : window_description.partition_by_actions)
            if (actions && actions->hasCorrelatedColumns())
                return true;
        for (const auto & actions : window_description.order_by_actions)
            if (actions && actions->hasCorrelatedColumns())
                return true;
        return false;
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const WindowDescription & getWindowDescription() const;
    const std::vector<WindowFunctionDescription> & getWindowFunctions() const { return window_functions; }

    /// Enable streaming mode: instead of the full sort + `WindowTransform` pipeline,
    /// use `StreamingLagTransform`.  Valid only when all window functions are
    /// `lagInFrame` with offset 1 and the input arrives sorted by
    /// `prefix_description + window ORDER BY` (storage ordering).
    /// `default_values[i]` holds the explicit default for function i, or `std::nullopt`
    /// when no default was specified (result uses `insertDefault`).
    void enableStreamingMode(
        SortDescription prefix_description_,
        std::vector<std::string> suffix_partition_col_names_,
        std::vector<std::string> value_col_names_,
        std::vector<std::optional<Field>> default_values_);

    bool isStreamingMode() const { return streaming_mode_; }

private:
    void updateOutputHeader() override;

    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
    bool streams_fan_out;

    bool streaming_mode_ = false;
    SortDescription streaming_prefix_description_;
    std::vector<std::string> streaming_suffix_partition_col_names_;
    std::vector<std::string> streaming_value_col_names_;
    std::vector<std::optional<Field>> streaming_default_values_;
};

}
