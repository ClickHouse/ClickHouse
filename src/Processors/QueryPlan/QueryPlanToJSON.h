#pragma once

#include <Common/JSONBuilder.h>
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
JSONBuilder::ItemPtr queryPlanToJSON(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names);

}
