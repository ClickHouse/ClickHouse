#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/TopKThresholdTracker.h>

namespace DB
{

class TopNAggregatingStep : public ITransformingStep
{
public:
    TopNAggregatingStep(
        const SharedHeader & input_header_,
        Names key_names_,
        AggregateDescriptions aggregates_,
        SortDescription sort_description_,
        size_t limit_,
        bool sorted_input_,
        bool enable_threshold_pruning_ = false,
        TopKThresholdTrackerPtr threshold_tracker_ = nullptr,
        /// For Mode 1: the input column by which the table is physically sorted
        /// (= the ORDER BY aggregate's argument, resolved to the pre-aggregation header).
        /// Used to build a MergingSortedTransform when N sorted streams need merging.
        String order_arg_col_name_ = {});

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
    bool enable_threshold_pruning;
    TopKThresholdTrackerPtr threshold_tracker;
    String order_arg_col_name;
};

}
