#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/WindowDescription.h>

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

    /// Window functions are always computed on the initiator (never on replicas), so a plan with a
    /// window function must still be recognized as "simple enough" for the automatic parallel replicas
    /// optimization (see `considerEnablingParallelReplicas`), otherwise it is rejected outright and no
    /// statistics are collected. Hence we report support here.
    ///
    /// Note that, unlike `SortingStep`/`LimitStep`, `WindowStep` does NOT attach a
    /// `RuntimeDataflowStatisticsCollector` in `transformPipeline`: it can never be the replica-output
    /// boundary (replicas ship the pre-window columns, and the window result columns appended above are
    /// never sent to the initiator), so instrumenting it would count the window result as replica output
    /// and inflate the cost model.
    ///
    /// For a window function over a bare table scan the replica-output boundary would be the reading
    /// step itself, which records only input bytes, so output bytes cannot be estimated there;
    /// `considerEnablingParallelReplicas` skips the optimization for such plans instead of feeding
    /// `output_bytes = 0` (i.e. "shipping all pre-window rows to the initiator is free") into the cost
    /// model. Statistics are collected when a proper boundary exists below the window, e.g. an
    /// aggregation whose result the window function consumes.
    bool supportsDataflowStatisticsCollection() const override { return true; }

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const WindowDescription & getWindowDescription() const;

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
    bool streams_fan_out;
};

}
