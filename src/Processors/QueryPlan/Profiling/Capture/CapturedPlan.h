#pragma once

#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanIndexStats.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>
#include <base/types.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class QueryPlan;
class AnalyzeStepsStats;
struct ExplainPlanOptions;
struct PrettyNamesPerPlan;

struct CapturedStep
{
    String id;
    String type;
    String description;
    VectorWithMemoryTracking<String> details;

    /// Ids of this step's children, and of the roots of any plans it has already built
    /// (`getBuiltChildPlans`).
    VectorWithMemoryTracking<String> children;

    std::optional<AnalyzedStepData> statistics;

    /// What index and projection analysis decided. Only a `ReadFromMergeTree` has any.
    PlanIndexStats indexes;
    PlanProjectionStats projections;
};

/// Where a query that threw while executing failed.
struct CapturedFailure
{
    /// Id of the node the failing processor belongs to. Empty when that is not a node of the plan:
    /// a processor added after the plan was built has no step, and a step of a plan that is not
    /// captured (such as a subquery's) has no node.
    String node_id;
    /// The stage of that node, matching a `GroupId` of its statistics. Meaningful only with `node_id`.
    size_t group_id = 0;
    String processor_name;
};

/// Statistics a query collects only when asked to, because collecting them costs extra.
struct CollectedStatistics
{
    /// Work intervals of the processors, which the per-step `Time` and `Concurrency` groups come from.
    bool work_intervals = false;
    /// The extra bookkeeping in joins behind the matched rows metrics that cannot be derived.
    bool join_matches = false;
};

/// A whole query: its own steps and the totals.
struct CapturedPlan
{
    /// Id of the root node
    String root_id;
    /// Output columns
    VectorWithMemoryTracking<String> output;
    /// Execution totals
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;
    /// Execution time split by what the threads were doing. Only when the work intervals were collected.
    std::optional<ExecutionTimeBreakdown> execution_time_breakdown;
    /// Set only when the processor that threw is known; absent does not mean the query succeeded.
    std::optional<CapturedFailure> failure;

    /// What the query collected beyond the default statistics, so a reader can tell a metric that
    /// was not collected from one the step does not have.
    CollectedStatistics collected;

    VectorWithMemoryTracking<CapturedStep> nodes;

    /// Reads a walked plan into a `CapturedPlan`. The walk gives the shape; each step is read
    /// here for what it says about itself.
    ///
    /// The walk must have been taken after the plan's pipeline was built and while the plan is
    /// still alive: `buildQueryPipeline` optimizes the plan, so the shape that will run is only
    /// visible afterwards, as are the index analysis and the child plans built along the way.
    /// `pretty_names` is the one input that has to have been read *before* the pipeline was
    /// built, since building it moves the `ActionsDAG` out of every expression step.
    ///
    /// `steps_to_stats` fills the statistics in as the plan is read, for a caller that already
    /// has them. The logged plan does not: it captures before the query runs and fills them in
    /// afterwards, through `QueryPlanLogCapture::captureStatistics`.
    static CapturedPlan capture(
        const PlanWalk & walk,
        const ExplainPlanOptions & options,
        size_t max_description_length,
        const AnalyzeStepsStats * steps_to_stats,
        const PrettyNamesPerPlan * pretty_names);
};

}
