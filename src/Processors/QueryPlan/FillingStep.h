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
    /// `sort_description` is the whole `ORDER BY`; the columns to fill are its `WITH FILL` elements.
    FillingStep(
        SharedHeader input_header_,
        SortDescription sort_description_,
        InterpolateDescriptionPtr interpolate_description_,
        bool use_with_fill_by_sorting_prefix);

    [[nodiscard]] String getName() const override { return "Filling"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    [[nodiscard]] bool supportsDataflowStatisticsCollection() const override { return true; }

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    [[nodiscard]] const SortDescription & getSortDescription() const override { return sort_description; }
    [[nodiscard]] const InterpolateDescriptionPtr & getInterpolateDescription() const { return interpolate_description; }

    void serialize(Serialization & ctx) const override;
    [[nodiscard]] bool isSerializable() const override { return true; }
    [[nodiscard]] QueryPlanStepPtr clone() const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    SortDescription sort_description;
    InterpolateDescriptionPtr interpolate_description;
    const bool use_with_fill_by_sorting_prefix;
};

}
