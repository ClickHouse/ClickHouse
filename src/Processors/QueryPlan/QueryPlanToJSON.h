#pragma once

#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/StepStatsModel.h>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>
#include <base/types.h>

#include <cstddef>


namespace DB
{

/// An executed plan, with the statistics it produced, for `system.query_log.query_plan`.
///
/// Capturing and writing are separate: `capturePlan` takes the plan apart into owned values while
/// the plan and its pipeline are alive, and `capturedPlanToJSON` writes the document from those
/// values afterwards, when neither still exists.
///
/// The document is a flat array of nodes rather than a tree:
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

/// One step, taken out of the plan while the plan and its pipeline are still alive.
///
/// Everything here is an owned value: nothing points into the plan, the pipeline or the steps, so
/// a captured plan stays readable after all three are gone. That is the point of capturing at all
/// -- `StepStatsStorage::analyzeStep` reads processors belonging to the pipeline and state held by
/// the step, neither of which survives the query, while the document is written much later.
struct CapturedStep
{
    String id;
    String type;
    String description;
    std::vector<String> details;

    /// Ids of this step's children, and of the roots of any plans it owns (`getChildPlans`), which
    /// are referenced the same way. Depth is expressed by reference, never by nesting.
    std::vector<String> children;

    std::vector<size_t> consumed_subquery_ids;

    /// Set when the step belongs to a captured sub-plan rather than to the query's own tree.
    std::optional<size_t> sub_plan_id;

    std::optional<AnalyzedStepData> statistics;

    /// `describeIndexes` and `describeProjections` write JSON directly and have no neutral form, so
    /// this is the one part of a capture that is already committed to a format. Owned all the same,
    /// so it does not extend the window in which the plan must be alive.
    std::unique_ptr<JSONBuilder::JSONMap> described;
};

/// A plan that ran for the query but is not part of its plan tree.
///
/// An `IN (SELECT ...)` whose set is built during planning -- so that index analysis can use it --
/// runs in a pipeline of its own before the main one exists, and nothing links it into the tree. Its
/// rows still count towards the query, so a document that omitted it would describe a query reading
/// sixty million rows without naming the table they came from.
struct CapturedSubPlan
{
    /// Identifies the subquery within the query. Every step that consumes its result carries the
    /// same id (`IQueryPlanStep::getConsumedSubqueryIds`), which is how the document names the
    /// consumers: the id is assigned once where the subquery is created and read at both ends,
    /// never recomputed, so the two ends cannot disagree.
    size_t subquery_id = 0;
    SubPlanKind kind = SubPlanKind::Set;

    String root_id;
    std::vector<CapturedStep> nodes;

    /// Totals for the sub-plan's own pipeline. The execution time is the subquery's own: it ran
    /// before the main pipeline existed, so it is not part of the query's execution time and the
    /// two do not add up.
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;
};

/// A whole query, captured: its own steps, the sub-plans that ran for it, and the totals. Enough to
/// write the document from, and nothing else.
struct CapturedPlan
{
    String root_id;
    std::vector<String> output;
    std::optional<UInt64> execution_time_ns;
    std::optional<UInt64> max_threads;

    std::vector<CapturedStep> nodes;
    std::vector<CapturedSubPlan> sub_plans;
};

/// Takes a plan apart into owned values.
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
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names);

/// The same for a sub-plan, whose pipeline is its own and finishes long before the query does.
CapturedSubPlan captureSubPlanData(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    size_t subquery_id,
    SubPlanKind kind,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names);

/// Writes the document. Takes the capture by non-const reference because it moves the `described`
/// maps out of it rather than copying them -- `JSONBuilder` items are not copyable, and a capture
/// is rendered once.
JSONBuilder::ItemPtr capturedPlanToJSON(CapturedPlan & captured);

}
