#pragma once

#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/PlanIndexStats.h>
#include <Processors/QueryPlan/StepStatisticsModel.h>
#include <base/types.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>


namespace DB
{

class QueryPlan;
class StepStatisticsCollector;
struct ExplainPlanOptions;
struct PrettyNamesPerPlan;

/// What kind of subquery a captured sub-plan came from.
enum class SubPlanKind
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

    /// Ids of this step's children, and of the roots of any plans it owns (`getChildPlans`).
    std::vector<String> children;

    /// Id of the subqueries this step reads.
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
};

/// Captures the plan into a `CapturedPlan` object
///
/// Must run after that plan's pipeline has executed and while it is still alive, for two reasons:
/// `buildQueryPipeline` optimizes the plan, so the shape that ran is only visible afterwards, and
/// the statistics are read from the pipeline's processors. `pretty_names` is the one input that has
/// to have been captured *before* the pipeline was built, since building it moves the `ActionsDAG`
/// out of every expression step.
CapturedPlan capturePlan(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatisticsCollector * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names);

/// The same for a sub-plan, whose pipeline is its own and finishes before the query does.
CapturedSubPlan captureSubPlanData(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    size_t subquery_id,
    SubPlanKind kind,
    const StepStatisticsCollector * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names);

}
