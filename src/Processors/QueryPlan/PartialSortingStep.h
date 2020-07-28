#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class PartialSortingStep : public ITransformingStep
{
public:
    explicit PartialSortingStep(
            const DataStream & input_stream,
            SortDescription sort_description_,
            UInt64 limit_,
            SizeLimits size_limits_);

    String getName() const override { return "PartialSorting"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    SortDescription sort_description;
    UInt64 limit;
    SizeLimits size_limits;
};

}
