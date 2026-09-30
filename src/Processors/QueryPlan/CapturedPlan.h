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

struct CapturedStep
{
    String id;
    String type;
    String description;
    std::vector<String> details;

    /// Ids of this step's children, and of the roots of any plans it owns (`getChildPlans`).
    std::vector<String> children;

    std::optional<AnalyzedStepData> statistics;

    /// What index analysis decided. Only a `ReadFromMergeTree` has any.
    PlanIndexStats indexes;
};

/// A whole query: its own steps and the totals.
struct CapturedPlan
{
    /// Id of the root node
    String root_id;
    /// Output columns
    std::vector<String> output;
    /// Execution totals
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;

    std::vector<CapturedStep> nodes;};

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

}
