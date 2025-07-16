#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Unfortunately the logic of this view is not easy to express with
/// default SQL operators in effective way. All attempts with GROUP BY are terribly slow and resource consuming.
/// That is why it's not just a normal StorageView, but a custom pipeline on top of StorageView.
///
/// This is one of the steps of this custom pipeline. It's public because we need to allow
/// filter push down through it and it's possible only with custom code in filterPushDown.cpp
class CustomMetricLogViewStep : public ITransformingStep
{
    SortDescription sort_description;
public:
    CustomMetricLogViewStep(Block input_header_, Block output_header_);

    String getName() const override
    {
        return "CustomMetricLogViewStep";
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void updateOutputHeader() override {}

    const SortDescription & getSortDescription() const override;
};


}
