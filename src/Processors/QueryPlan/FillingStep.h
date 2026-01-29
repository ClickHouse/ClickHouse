#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <Core/InterpolateDescription.h>

namespace DB
{

/// Implements modifier WITH FILL of ORDER BY clause. See FillingTransform.
class FillingStep : public ITransformingStep
{
public:
    FillingStep(
        const Header & input_header_,
        SortDescription sort_description_,
        SortDescription fill_description_,
        InterpolateDescriptionPtr interpolate_description_,
        bool use_with_fill_by_sorting_prefix);

    String getName() const override { return "Filling"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortDescription & getSortDescription() const override { return sort_description; }

private:
    void updateOutputHeader() override;

    SortDescription sort_description;
    SortDescription fill_description;
    InterpolateDescriptionPtr interpolate_description;
    const bool use_with_fill_by_sorting_prefix;
};

}
