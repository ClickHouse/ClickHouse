#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/IEJoinTransform.h>

namespace DB
{

/// Joins two data streams by two inequality conditions with the IEJoin algorithm
/// (see `IEJoinTransform`). Inputs are consumed unsorted; the output is the concatenation
/// of the left and right input columns.
class IEJoinStep : public IQueryPlanStep
{
public:
    IEJoinStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        IEJoinConditions conditions_,
        size_t max_block_size_);

    String getName() const override { return "IEJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    String formatConditions() const;

    IEJoinConditions conditions;
    size_t max_block_size;
};

}
