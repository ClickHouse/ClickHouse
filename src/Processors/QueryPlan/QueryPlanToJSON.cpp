#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/PlanIndexStats.h>
#include <Processors/QueryPlan/StepStatisticsJSONPrinter.h>
#include <base/types.h>

#include <memory>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace
{

/// One node of the flat `Nodes` array.
/// `sub_plan_id` marks which plan the step came from, which the document has to say because it
/// flattens every plan into one `Nodes` array.
JSONBuilder::ItemPtr capturedStepToJSON(const CapturedStep & step, std::optional<size_t> sub_plan_id = {})
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
    if (auto projections = projectionStatsToJSON(step.projections))
        map->add("Projections", std::move(projections));

    if (sub_plan_id)
        map->add("SubPlanId", *sub_plan_id);

    auto children = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & child : step.children)
        children->add(child);
    map->add("Children", std::move(children));

    return map;
}

/// One entry of `SubPlans`, shaped like the root of the document one level down: same keys, same
/// meanings, for the pipeline this subquery ran in. `ExecutionTimeNs` is that pipeline's own -- it
/// ran before the main one existed, so it is not part of the query's and the two do not sum.
JSONBuilder::ItemPtr capturedSubPlanToJSON(
    const CapturedSubPlan & sub_plan,
    const std::unordered_map<size_t, std::vector<String>> & consuming_step_ids_by_subquery_id)
{
    auto entry = std::make_unique<JSONBuilder::JSONMap>();
    entry->add("Id", sub_plan.subquery_id);
    entry->add("Kind", String(toString(sub_plan.kind)));
    entry->add("Root", sub_plan.root_id);

    /// Which steps use this subquery's result. Absent when nothing in the document reads it -- a
    /// set used only by index analysis, say.
    if (const auto it = consuming_step_ids_by_subquery_id.find(sub_plan.subquery_id);
        it != consuming_step_ids_by_subquery_id.end())
    {
        auto consumers = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & step_id : it->second)
            consumers->add(step_id);
        entry->add("ConsumedBy", std::move(consumers));
    }

    if (sub_plan.execution_time_ns)
        entry->add("ExecutionTimeNs", *sub_plan.execution_time_ns);
    if (sub_plan.max_threads)
        entry->add("MaxThreads", *sub_plan.max_threads);

    return entry;
}

}

JSONBuilder::ItemPtr capturedPlanToJSON(const CapturedPlan & captured)
{
    /// Maps a subquery id to the ids of the steps that read its result.
    std::unordered_map<size_t, std::vector<String>> consuming_step_ids_by_subquery_id;

    const auto collectConsumers = [&](const std::vector<CapturedStep> & steps)
    {
        for (const auto & step : steps)
            for (size_t subquery_id : step.consumed_subquery_ids)
                consuming_step_ids_by_subquery_id[subquery_id].push_back(step.id);
    };

    collectConsumers(captured.nodes);
    for (const auto & sub_plan : captured.sub_plans)
        collectConsumers(sub_plan.nodes);

    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & node : captured.nodes)
        nodes_array->add(capturedStepToJSON(node));

    /// Plans that ran for this query without being part of its tree: described at the root, with
    /// their steps listed among the others so a reader walks one array.
    auto sub_plan_entries = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & sub_plan : captured.sub_plans)
    {
        sub_plan_entries->add(capturedSubPlanToJSON(sub_plan, consuming_step_ids_by_subquery_id));

        for (const auto & step : sub_plan.nodes)
            nodes_array->add(capturedStepToJSON(step, sub_plan.subquery_id));
    }

    auto result = std::make_unique<JSONBuilder::JSONMap>();
    result->add("Version", QUERY_PLAN_JSON_VERSION);
    result->add("Root", captured.root_id);

    if (captured.execution_time_ns)
        result->add("ExecutionTimeNs", *captured.execution_time_ns);
    if (captured.max_threads)
        result->add("MaxThreads", *captured.max_threads);

    auto output_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : captured.output)
        output_array->add(column);
    result->add("Output", std::move(output_array));

    if (!captured.sub_plans.empty())
        result->add("SubPlans", std::move(sub_plan_entries));

    result->add("Nodes", std::move(nodes_array));

    return result;
}

}
