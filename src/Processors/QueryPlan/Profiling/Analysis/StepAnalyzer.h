#pragma once

#include <functional>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <base/types.h>

namespace DB
{

class IProcessor;
/// The processors that ran one step.
using StepProcessors = std::span<IProcessor * const>;

/// Builds the raw report of one step from the processors that ran it. Counters that live in an
/// object shared with the processors, such as an `IJoin`, are reached through a captured
/// `shared_ptr` to it.
using StepReporter = std::function<StepAnalysisReport(StepProcessors)>;

struct StepStatsContext;

/// Shapes a step's raw report into its analysis, adding what the step itself knows -- a join's
/// estimation and input columns, for instance -- to what was measured.
using StepFinisher = std::function<AnalyzedStepData(const StepStatsContext &, StepAnalysisReport)>;

/// The rows a join matched, read from its raw report and the rows it output. Branch costs are
/// accumulated from these, so a step that sets this is a step that takes part in that pass.
using JoinCardinalityReader = std::function<std::optional<UInt64>(const StepAnalysisReport &, UInt64)>;

/// Analyses one step. Built by the step while the plan is alive, run after the plan is gone, so
/// it holds everything it needs by value or by `shared_ptr` and never reaches back into the step.
///
/// It is run in three phases, because the branch cost of a join is accumulated from the reports of
/// the joins below it and so cannot be computed while those reports are still being gathered:
///   1. `reporter` on every step, giving each step's raw report;
///   2. the branch costs, over all of the join reports at once;
///   3. `finish` on every step, shaping its own report into `AnalyzedStepData`.
/// The stages are read before all of it, to gather the per-stage timings the context carries.
struct StepAnalyzer
{
    /// `IQueryPlanStep::getUniqID` of the step this analyses.
    String id;

    /// The steps below this one: its own children, then the roots of the child plans it holds.
    /// Filled by the walk, which is the only thing that knows the shape.
    std::vector<String> children;

    /// What the passes that run across steps need to know about this one. A step declares these
    /// for itself, so that nothing has to ask what kind of step it is after the fact.
    ///
    /// Joins of one cluster share their cost. Zero means the join belongs to no cluster.
    UInt64 cluster_id = 0;
    /// A set's builder and its consumers share one spill scope, and the total is attributed to
    /// the builder when there is one.
    bool is_creating_set = false;

    /// Every step has at least the one unnamed stage.
    std::vector<size_t> groups{0};
    std::unordered_map<size_t, String> group_names;

    /// Empty when the step has nothing to report of its own.
    StepReporter reporter;

    StepFinisher finish;

    /// Set by joins only; its presence is what puts a step in the branch-cost phase.
    JoinCardinalityReader join_cardinality;

    /// What a pass across steps asks. The fields above are what a step declares about itself;
    /// these are the questions asked of it afterwards, so no caller has to know which field
    /// answers which question.
    bool isJoin() const { return join_cardinality != nullptr; }
    bool isCreatingSet() const { return is_creating_set; }
};

}
