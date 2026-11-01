#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <memory>
#include <optional>
#include <vector>

/// The types and constants that the files of the multi-pass top-K aggregation share (see `addMultiPassTopK`).

namespace DB
{

/// A key that bounds the ranks of the groups of a query of several keys: a value of it holds all the rows of its
/// groups, so a value of fewer rows than the n-th best rank holds no group of the top.
struct TopKBoundingKey
{
    /// The key in the input of the aggregation, and the column of the read that it is.
    String name;
    String column;
    DataTypePtr type;
    /// The column is part of the primary or the partition key, so a pass over some of its values reads only their
    /// granules.
    bool prunes_granules = false;
};

/// What a multi-pass top-K aggregation takes from the plan: the top of the query, and the input of the aggregation, which
/// it reads again for each of its passes: copies of the reading step, which carries the analysis of the ranges that
/// every pass reads, and of the expressions and filters between it and the aggregation, from the bottom up.
struct MultiPassTopKInput
{
    /// The number of the best groups the query takes, `LIMIT` plus `OFFSET`, and the position of the aggregate that
    /// ranks them.
    size_t top = 0;
    size_t rank_index = 0;

    std::unique_ptr<ReadFromMergeTree> read;
    std::vector<QueryPlanStepPtr> input_steps;
    /// Set for a query of several keys that one of them bounds (see `TopKBoundingKey`).
    std::optional<TopKBoundingKey> bounding_key;
    /// The context of the query, which the passes read and plan with.
    ContextPtr context;
};

namespace MultiPassTopKImpl
{

/// What the passes of a multi-pass top-K aggregation plan from: the input, which every pass reads again, and a copy of
/// the original aggregation, which every pass clones with parameters of its own.
struct MultiPassTopKTemplate
{
    MultiPassTopKInputPtr input;
    std::unique_ptr<AggregatingStep> aggregating;

    /// The number of the best groups the query takes: `LIMIT` plus `OFFSET`.
    size_t top = 0;
    /// The aggregate that ranks the groups: a lone `count`, `uniqExact` or `uniqExactIf`.
    size_t rank_index = 0;
    /// A `count()` among the aggregates, which gives the rows of a group.
    std::optional<size_t> rows_index;
};

using MultiPassTopKTemplatePtr = std::shared_ptr<const MultiPassTopKTemplate>;

/// The candidates are at most this many of the keys with the best sampled ranks.
constexpr size_t max_candidates = 4096;

/// A key is a candidate when its sampled rank reaches the n-th best sampled rank divided by this: a key of
/// the top whose share of the sample fell short of its share of the table by this factor still is one.
constexpr UInt64 candidate_rank_divisor = 4;

/// The traits of the steps of the passes that keep every row of their input, and of those that filter it.
inline const ITransformingStep::Traits row_preserving_traits{
    .data_stream_traits = {.returns_single_stream = false, .preserves_number_of_streams = true, .preserves_sorting = true},
    .transform_traits = {.preserves_number_of_rows = true}};

inline const ITransformingStep::Traits filtering_traits{
    .data_stream_traits = {.returns_single_stream = false, .preserves_number_of_streams = true, .preserves_sorting = true},
    .transform_traits = {.preserves_number_of_rows = false}};

}

}
