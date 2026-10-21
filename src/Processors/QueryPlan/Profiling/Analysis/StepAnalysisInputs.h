#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Core/Joins.h>
#include <Processors/QueryPlan/JoinEstimation.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <base/types.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// What the analysis of a join needs from the step, read out while the step is alive.
struct JoinAnalysisInputs
{
    /// A block nested loop join is a step of its own rather than an `IJoin`, so it reports its kind
    /// directly; the other two carry the join and read the kind from its `TableJoin`.
    bool is_block_nested_loop = false;

    JoinPtr join;
    JoinKind block_nested_loop_kind = JoinKind::Inner;
    JoinStrictness block_nested_loop_strictness = JoinStrictness::Unspecified;

    /// Only a `JoinStep` can swap its inputs, and only a `JoinStep` carries an estimation.
    bool can_swap_streams = false;
    bool swapped = false;
    bool has_estimation = false;
    JoinEstimation estimation;

    /// The input columns of the two sides, already shaped into metric groups.
    std::vector<MetricGroup> input_columns;
};

/// Everything the analysis reads from a step, as values. Taken while the plan is alive so the
/// statistics can be shaped once it is gone; `EXPLAIN ANALYZE` takes it from the step it is
/// already holding, and the logged plan takes it when the plan is captured.
struct StepAnalysisInputs
{
    /// The stages of the step. Every step has at least the one unnamed stage.
    std::vector<size_t> groups{0};
    std::unordered_map<size_t, String> group_names;

    /// Set only for the steps whose statistics are shaped as a join's.
    std::optional<JoinAnalysisInputs> join;
};

}
