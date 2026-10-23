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
#include <vector>

namespace DB
{

class QueryPlan;
class AnalyzeStepsStats;
struct ExplainPlanOptions;
struct PrettyNamesPerPlan;

struct CapturedStep
{
    UInt64 id = 0;
    String type;
    String description;
    std::vector<String> details;

    /// Ids of this step's children, and of the roots of any plans it has already built
    /// (`getBuiltChildPlans`).
    std::vector<UInt64> children;

    std::optional<AnalyzedStepData> statistics;

    /// What index and projection analysis decided. Only a `ReadFromMergeTree` has any.
    PlanIndexStats indexes;
    PlanProjectionStats projections;
};

/// A whole query: its own steps and the totals.
struct CapturedPlan
{
    /// Id of the root node
    UInt64 root_id = 0;
    /// Output columns
    std::vector<String> output;
    /// Execution totals
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;

    std::vector<CapturedStep> nodes;

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
    /// afterwards, through `QueryPlanProfiler::captureStatistics`.
    static CapturedPlan capture(
        const PlanWalk & walk,
        const ExplainPlanOptions & options,
        size_t max_description_length,
        const AnalyzeStepsStats * steps_to_stats,
        const PrettyNamesPerPlan * pretty_names);
};

}
