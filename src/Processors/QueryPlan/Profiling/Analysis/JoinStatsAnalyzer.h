#pragma once

#include <memory>
#include <vector>
#include <Core/Joins.h>
#include <Processors/QueryPlan/JoinEstimation.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// What shaping a join's report needs from the step, read out while the step is alive.
struct JoinAnalysisInputs
{
    /// `BlockNestedLoopJoinStep` is a step of its own rather than an `IJoin`, so it states its kind
    /// here; `JoinStep` and `FilledJoinStep` carry the join and read the kind from its `TableJoin`.
    bool is_block_nested_loop = false;

    JoinPtr join;
    JoinKind block_nested_loop_kind = JoinKind::Inner;
    JoinStrictness block_nested_loop_strictness = JoinStrictness::Unspecified;

    /// Set by `JoinStep` only: it is the only join step that swaps its inputs or has an estimation.
    bool swapped = false;
    bool has_estimation = false;
    JoinEstimation estimation;

    /// The input columns of the two sides, already shaped into metric groups.
    std::vector<MetricGroup> input_columns;
};

/// A report generator that knows some join specific information and builds the report
/// with metrics for printing in EXPLAIN ANALYZE
AnalyzedStepData analyzeJoinStep(const StepStatsContext & context, StepAnalysisReport report, const JoinAnalysisInputs & join_inputs);

/// Actual matched pairs, i.e. the output rows produced by real matches (excluding NULL-padded
/// rows of the preserved sides of an outer join)
std::optional<UInt64> joinMatchedOutputRows(
    const StepAnalysisReport & report, UInt64 output_rows, JoinKind kind, JoinStrictness strictness);

}
