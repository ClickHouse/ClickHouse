#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Fused GROUP BY ... ORDER BY aggregate LIMIT K step for sorted input (Mode 1).
///
/// When the MergeTree table is physically sorted by the ORDER BY aggregate's
/// argument, each group's aggregate result is determined by its first occurrence
/// when reading in order. Aggregation is delegated to the standard Aggregator;
/// the transform stops after K distinct groups, so only O(K) rows are read.
class TopNAggregatingStep : public ITransformingStep
{
public:
    TopNAggregatingStep(
        const SharedHeader & input_header_,
        Aggregator::Params params_,
        SortDescription sort_description_,
        size_t limit_,
        String order_arg_col_name_ = {});

    String getName() const override { return "TopNAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortDescription & getSortDescription() const override { return sort_description; }

    size_t getLimit() const { return limit; }

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;

    Aggregator::Params params;
    SortDescription sort_description;
    size_t limit;

    /// The input column by which the table is physically sorted
    /// (= the ORDER BY aggregate's argument, resolved to the pre-aggregation header).
    /// Used to build a MergingSortedTransform when N sorted streams need merging.
    String order_arg_col_name;
};

}
