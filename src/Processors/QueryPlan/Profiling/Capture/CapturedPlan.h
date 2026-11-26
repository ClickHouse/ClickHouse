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

    /// Hash of the shape of the subtree under this step, see `hashQueryPlanShape`. Absent for a step
    /// that did not exist yet when the plan was hashed, such as one in a child plan built while the
    /// pipeline was.
    std::optional<UInt64> shape_hash;
};

/// A whole query: its own steps and the totals.
struct CapturedPlan
{
    /// Id of the root node
    String root_id;
    /// Hash of the shape of the whole plan, see `hashQueryPlanShape`: equal for two plans that differ
    /// only in their constants.
    std::optional<UInt64> shape_hash;
    /// Output columns
    VectorWithMemoryTracking<String> output;
    /// Execution totals
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;

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
