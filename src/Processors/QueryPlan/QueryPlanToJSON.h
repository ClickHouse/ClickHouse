#pragma once

#include <Common/JSONBuilder.h>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>
#include <base/types.h>

#include <cstddef>


namespace DB
{

/// Written into every plan as `Version`, so a reader can tell what it is looking at rather than
/// guessing from which keys happen to be present.
///
/// Bump only when a reader written against the previous version would misread this one -- a key
/// renamed, removed, or given a different meaning. Adding a key is not such a change: a reader
/// that does not know it ignores it, and rows written by older servers stay readable either way,
/// because `system.query_log` holds whatever version was current when each row was written.
constexpr UInt64 QUERY_PLAN_JSON_VERSION = 1;

class QueryPlan;
class StepStatsStorage;
struct ExplainPlanOptions;
struct PrettyNamesPerPlan;

/// Serializes an executed plan, with the statistics it produced, for `system.query_log.query_plan`.
///
/// The result is a flat array of nodes rather than a tree:
///
///     {"Version": 1, "Root": "<id>", "Output": [...],
///      "Nodes": [{"Node Id": ..., "Children": [...], ...}]}
///
/// A tree would nest to the depth of the plan, and nothing in SQL can walk an arbitrary depth, so
/// the question the column exists to answer -- which steps spend the time, across many queries --
/// would be unanswerable. Flat, it is one `arrayJoin` over `Nodes`. Sub-plans (`getChildPlans`,
/// as a `Merge` table produces) are ordinary entries referenced from a parent's `Children`, not a
/// second kind of nesting.
///
/// Separate from `QueryPlan::explainPlan` on purpose: that one serves `EXPLAIN json=1`, whose
/// output is a tree and must stay as it is.
///
/// Must run while the pipeline that produced `steps_to_stats` is alive, because the statistics are
/// read from its processors. `pretty_names` has to have been captured before the pipeline was
/// built, since building it moves the `ActionsDAG` out of every expression step.
/// What kind of subquery a captured sub-plan came from. Rendered as `Kind`, so a reader can tell
/// them apart without parsing prose, and so more kinds can be added without a format change.
enum class SubPlanKind
{
    /// An `IN (SELECT ...)` whose set is built during planning, so that index analysis can use it.
    Set,
    /// A `(SELECT ...)` yielding a single value, executed during analysis and folded into the outer
    /// query as a literal. Nothing of it survives in the plan -- not even a reference -- so without
    /// capturing it the document describes a query that reads from `system.one`.
    Scalar,
};

std::string_view toString(SubPlanKind kind);

/**
 * A plan that ran for the query but is not part of its plan tree.
 *
 * An `IN (SELECT ...)` whose set is built during planning -- so that index analysis can use it --
 * runs in a pipeline of its own before the main one exists, and nothing links it into the tree. Its
 * rows still count towards the query, so a document that omitted it would describe a query reading
 * sixty million rows without naming the table they came from.
 */
struct SerializedSubPlan
{
    /// Identifies the subquery within the query. Every step that consumes its result carries the
    /// same id (`IQueryPlanStep::getConsumedSubqueryIds`), which is how the document names the
    /// consumers: the id is assigned once where the subquery is created and read at both ends,
    /// never recomputed, so the two ends cannot disagree.
    size_t subquery_id = 0;
    SubPlanKind kind = SubPlanKind::Set;

    /// Id of the sub-plan's root node, so the document can point at it.
    String root_id;
    std::vector<JSONBuilder::ItemPtr> nodes;

    /// `(node id, ids of the subqueries that node's step consumes)` for this sub-plan's own steps.
    /// Kept because a subquery can be consumed by another sub-plan rather than by the query's plan
    /// -- TPC-H Q20 nests exactly that way -- so the document's join has to see these too.
    std::vector<std::pair<String, std::vector<size_t>>> node_consumers;

    /// Totals for the sub-plan's own pipeline, set when it was serialized with statistics. The
    /// execution time is the subquery's own: it ran before the main pipeline existed, so it is not
    /// part of the query's execution time and the two do not add up.
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;
};

/// Serializes a sub-plan's nodes, with the statistics its own pipeline produced.
///
/// Runs after that pipeline has executed and while it is still alive, exactly as the main document
/// does, and for the same two reasons: `buildQueryPipeline` optimizes the plan, so the shape that
/// ran is only visible afterwards, and the statistics are read from the pipeline's processors.
/// `pretty_names` is the one input that has to be captured *before* the pipeline was built, since
/// building it moves the `ActionsDAG` out of every expression step.
SerializedSubPlan serializeSubPlan(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    size_t subquery_id,
    SubPlanKind kind,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names);

JSONBuilder::ItemPtr queryPlanToJSON(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names,
    std::vector<SerializedSubPlan> * sub_plans = nullptr);

}
