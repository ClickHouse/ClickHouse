#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

/// Sort separate chunks of data.
class PartialSortingStep : public ITransformingStep
{
public:
    explicit PartialSortingStep(
            const DataStream & input_stream,
            SortDescription sort_description_,
            UInt64 limit_,
            SizeLimits size_limits_);

    String getName() const override { return "PartialSorting"; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

private:
    SortDescription sort_description;
    UInt64 limit;
    SizeLimits size_limits;
};

}
