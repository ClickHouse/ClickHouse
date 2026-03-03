#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Fused step for GROUP BY ... ORDER BY aggregate LIMIT K.
/// Replaces the AggregatingStep + SortingStep + LimitStep chain.
/// Produces sorted output limited to K rows.
class TopNAggregatingStep : public ITransformingStep
{
public:
    TopNAggregatingStep(
        const SharedHeader & input_header_,
        Names key_names_,
        AggregateDescriptions aggregates_,
        SortDescription sort_description_,
        size_t limit_,
        bool sorted_input_);

    String getName() const override { return "TopNAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortDescription & getSortDescription() const override { return sort_description; }

    size_t getLimit() const { return limit; }
    bool isSortedInput() const { return sorted_input; }
    const Names & getKeyNames() const { return key_names; }
    const AggregateDescriptions & getAggregateDescriptions() const { return aggregates; }

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;

    Names key_names;
    AggregateDescriptions aggregates;
    SortDescription sort_description;
    size_t limit;
    bool sorted_input;
};

}
