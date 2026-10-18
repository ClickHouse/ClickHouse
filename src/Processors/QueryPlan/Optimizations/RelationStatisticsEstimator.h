#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{
class AggregatingStep;
}

namespace DB::QueryPlanOptimizations
{

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. Runtime filters prune nothing at plan time, so the estimate
/// ignores a predicate that holds whenever its runtime filters pass: a runtime filter, an `and` of
/// them, or `__applyFilter(...) OR isNull(key)`.
/// With `for_runtime_filter_transport` the estimate is a hard row cap for a transported filter's
/// exact phase, so an unindexed plan-time filter yields no estimate instead of a statistics guess.
RelationStats estimateReadRowsCount(
    QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, bool for_runtime_filter_transport = false);

/// The statistics profile of a `MergeTree` read under its `filter` and `PREWHERE`: the statistics
/// estimate, with the conditions the key analysis selects ranges by taken from that analysis when
/// the estimate took a default. `analysis` may be null; `metadata` is passed to the statistics estimator.
RelationProfile estimateReadProfile(
    const ReadFromMergeTree & reading,
    const ConditionSelectivityEstimator & estimator,
    const StorageMetadataPtr & metadata,
    const ReadFromMergeTree::AnalysisResult * analysis,
    const ActionsDAG::Node * filter,
    const ActionsDAG::Node * prewhere);

/// Rows of an aggregation or a `DISTINCT` over `keys`, by the shared group count formula; the output
/// columns are the keys with their NDVs, zero for a key without one.
RelationStats estimateGroupStats(const Names & keys, const RelationStats & input_stats);

/// The same for an aggregating step; `GROUPING SETS` count the groups of every set.
RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats);

}
