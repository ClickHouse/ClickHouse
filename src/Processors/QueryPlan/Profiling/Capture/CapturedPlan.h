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

/// What kind of subquery a captured sub-plan came from.
enum class SubPlanKind : uint8_t
{
    /// An `IN (SELECT ...)` whose set is built during planning, so that index analysis can use it.
    Set,
    /// A `(SELECT ...)` yielding a single value, executed during analysis and folded into the
    /// outer query as a literal.
    Scalar,
};

std::string_view toString(SubPlanKind kind);

struct CapturedStep
{
    String id;
    String type;
    String description;
    std::vector<String> details;

    /// Ids of this step's children, and of the roots of any plans it has already built
    /// (`getBuiltChildPlans`).
    std::vector<String> children;

    /// Ids of the subqueries whose results this step reads.
    std::vector<size_t> consumed_subquery_ids;

    std::optional<AnalyzedStepData> statistics;

    /// What index and projection analysis decided. Only a `ReadFromMergeTree` has any.
    PlanIndexStats indexes;
    PlanProjectionStats projections;
};

/// A plan that ran for the query but is not part of its plan tree.
struct CapturedSubPlan
{
    /// Identifies the subquery within the query. Every step that consumes its result carries the
    /// same id.
    size_t subquery_id = 0;
    SubPlanKind kind = SubPlanKind::Set;

    /// Id of the root node
    String root_id;
    std::vector<CapturedStep> nodes;

    /// Totals for the sub-plan's own pipeline.
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;

    /// Reads a walked sub-plan, under the same conditions as `CapturedPlan::capture`, and without
    /// statistics: they are filled in once the sub-plan's pipeline has run.
    static CapturedSubPlan capture(
        const PlanWalk & walk,
        const ExplainPlanOptions & options,
        size_t max_description_length,
        const PrettyNamesPerPlan * pretty_names,
        size_t subquery_id,
        SubPlanKind kind);
};

/// A whole query: its own steps, the sub-plans that ran for it, and the totals.
struct CapturedPlan
{
    /// Id of the root node
    String root_id;
    /// Output columns
    std::vector<String> output;
    /// Execution totals
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;

    std::vector<CapturedStep> nodes;
    std::vector<CapturedSubPlan> sub_plans;

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
