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
        String order_arg_col_name_ = {},
        bool merge_only_ = false,
        bool partial_only_ = false);

    String getName() const override { return "TopNAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortDescription & getSortDescription() const override { return sort_description; }

    size_t getLimit() const { return limit; }
    bool isSortedInput() const { return sorted_input; }
    bool isMergeOnly() const { return merge_only; }
    bool isPartialOnly() const { return partial_only; }
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

    /// For Mode 1: the input column by which the table is physically sorted
    /// (= the ORDER BY aggregate's argument, resolved to the pre-aggregation header).
    /// Used to build a MergingSortedTransform when N sorted streams need merging.
    String order_arg_col_name;

    /// Merge-only mode: input is already intermediate aggregate states (from
    /// AggregatingStep under parallel replicas). Creates only the merge transform.
    bool merge_only;

    /// Partial-only mode: creates only partial workers (no local merge).
    /// Output is intermediate aggregate states (same schema as AggregatingStep
    /// with final=false). Used for local replica under parallel replicas so the
    /// coordinator's merge_only step handles the final merge globally.
    bool partial_only;
};

}
