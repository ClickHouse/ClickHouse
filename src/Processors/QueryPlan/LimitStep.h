#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Executes LIMIT. See LimitTransform.
class LimitStep : public ITransformingStep
{
public:
    LimitStep(
        const DataStream & input_stream_,
        size_t limit_, size_t offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    String getName() const override { return "Limit"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    size_t limit;
    size_t offset;
    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;
};

}
