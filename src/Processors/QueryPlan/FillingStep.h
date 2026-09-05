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

    String getName() const override { return "Filling"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortDescription & getSortDescription() const override { return sort_description; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    QueryPlanStepPtr clone() const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    SortDescription sort_description;
    InterpolateDescriptionPtr interpolate_description;
    const bool use_with_fill_by_sorting_prefix;
};

}
