#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/PlanIndexStats.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatisticsJSONPrinter.h>
#include <base/types.h>

#include <memory>
#include <set>
#include <vector>

namespace DB
{

namespace
{

/// One node of a plan's `Nodes` array.
JSONBuilder::ItemPtr capturedStepToJSON(const CapturedStep & step, const std::set<size_t> & captured_subquery_ids)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    map->add("Node Type", step.type);
    map->add("Node Id", step.id);

    if (!step.description.empty())
        map->add("Description", step.description);

    auto details = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & line : step.details)
        details->add(line);
    map->add("Details", std::move(details));

    if (step.statistics)
        map->add("Statistics", StepStatisticsJSONPrinter::toJSON(*step.statistics));

    if (auto indexes = indexStatsToJSON(step.indexes))
        map->add("Indexes", std::move(indexes));

    /// The sub-plans whose results this step reads, by `SubqueryId`, which is how the plans of the
    /// document are linked. A subquery the step reads but whose plan was not captured -- a scalar
    /// served from the cache, say -- has no entry in `Plans` to point at, so it is not listed.
    /// Absent for the many steps that read none.
    std::set<size_t> sub_plan_ids;
    for (size_t subquery_id : step.consumed_subquery_ids)
        if (captured_subquery_ids.contains(subquery_id))
            sub_plan_ids.insert(subquery_id);

    if (!sub_plan_ids.empty())
    {
        auto sub_plans = std::make_unique<JSONBuilder::JSONArray>();
        for (size_t id : sub_plan_ids)
            sub_plans->add(id);
        map->add("SubPlans", std::move(sub_plans));
    }

    auto children = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & child : step.children)
        children->add(child);
    map->add("Children", std::move(children));

    return map;
}

std::unique_ptr<JSONBuilder::JSONArray> capturedStepsToJSON(
    const std::vector<CapturedStep> & steps, const std::set<size_t> & captured_subquery_ids)
{
    auto nodes = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & step : steps)
        nodes->add(capturedStepToJSON(step, captured_subquery_ids));
    return nodes;
}

/// The entry of `Plans` for the query's own plan, which is always the first one.
JSONBuilder::ItemPtr capturedQueryPlanToJSON(const CapturedPlan & captured, const std::set<size_t> & captured_subquery_ids)
{
    auto entry = std::make_unique<JSONBuilder::JSONMap>();
    entry->add("Kind", "Query");
    entry->add("Root", captured.root_id);

    if (captured.execution_time_ns)
        entry->add("ExecutionTimeNs", *captured.execution_time_ns);
    if (captured.max_threads)
        entry->add("MaxThreads", *captured.max_threads);

    auto output_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : captured.output)
        output_array->add(column);
    entry->add("Output", std::move(output_array));

    entry->add("Nodes", capturedStepsToJSON(captured.nodes, captured_subquery_ids));

    return entry;
}

/// The entry of `Plans` for a subquery that ran outside the query's plan tree. `ExecutionTimeNs` is
/// that subquery's own pipeline's -- it ran before the query's existed, so the two do not sum.
JSONBuilder::ItemPtr capturedSubPlanToJSON(const CapturedSubPlan & sub_plan, const std::set<size_t> & captured_subquery_ids)
{
    auto entry = std::make_unique<JSONBuilder::JSONMap>();
    entry->add("Kind", String(toString(sub_plan.kind)));
    entry->add("SubqueryId", sub_plan.subquery_id);
    entry->add("Root", sub_plan.root_id);

    if (sub_plan.execution_time_ns)
        entry->add("ExecutionTimeNs", *sub_plan.execution_time_ns);
    if (sub_plan.max_threads)
        entry->add("MaxThreads", *sub_plan.max_threads);

    entry->add("Nodes", capturedStepsToJSON(sub_plan.nodes, captured_subquery_ids));

    return entry;
}

}

JSONBuilder::ItemPtr capturedPlanToJSON(const CapturedPlan & captured)
{
    std::set<size_t> captured_subquery_ids;
    for (const auto & sub_plan : captured.sub_plans)
        captured_subquery_ids.insert(sub_plan.subquery_id);

    /// Every plan that ran for the query, each with its own nodes: the query's own plan first, then
    /// the subqueries that ran outside its tree.
    auto plans = std::make_unique<JSONBuilder::JSONArray>();
    plans->add(capturedQueryPlanToJSON(captured, captured_subquery_ids));
    for (const auto & sub_plan : captured.sub_plans)
        plans->add(capturedSubPlanToJSON(sub_plan, captured_subquery_ids));

    auto result = std::make_unique<JSONBuilder::JSONMap>();
    result->add("Version", QUERY_PLAN_JSON_VERSION);
    result->add("Plans", std::move(plans));

    return result;
}

}
