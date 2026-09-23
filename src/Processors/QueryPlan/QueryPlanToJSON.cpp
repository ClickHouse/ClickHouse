#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/PlanIndexStats.h>
#include <Processors/QueryPlan/StepStatsJSONPrinter.h>
#include <base/types.h>

#include <memory>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace
{

/// One node of the flat `Nodes` array.
JSONBuilder::ItemPtr capturedStepToJSON(const CapturedStep & step)
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
        map->add("Statistics", StepStatsJSONPrinter::toJSON(*step.statistics));

    if (auto indexes = indexStatsToJSON(step.indexes))
        map->add("Indexes", std::move(indexes));
    if (auto projections = projectionStatsToJSON(step.projections))
        map->add("Projections", std::move(projections));

    if (step.sub_plan_id)
        map->add("SubPlanId", *step.sub_plan_id);

    auto children = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & child : step.children)
        children->add(child);
    map->add("Children", std::move(children));

    return map;
}

}

JSONBuilder::ItemPtr capturedPlanToJSON(const CapturedPlan & captured)
{
    /// Which nodes consume each subquery. Both ends carry the same assigned id, so this is a join
    /// over ids rather than a search for a name in rendered text: a step records the ids of the
    /// subqueries whose sets it reads (`IQueryPlanStep::getConsumedSubqueryIds`), and each captured
    /// sub-plan knows which subquery it is.
    ///
    /// Gathered from the query's steps and from every sub-plan's steps before any entry is written,
    /// because a subquery can be consumed by a step in another sub-plan -- TPC-H Q20 nests exactly
    /// that way.
    std::unordered_map<size_t, std::vector<String>> consumers_by_subquery;

    const auto gather = [&](const std::vector<CapturedStep> & nodes)
    {
        for (const auto & node : nodes)
            for (size_t id : node.consumed_subquery_ids)
                consumers_by_subquery[id].push_back(node.id);
    };

    gather(captured.nodes);
    for (const auto & sub_plan : captured.sub_plans)
        gather(sub_plan.nodes);

    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & node : captured.nodes)
        nodes_array->add(capturedStepToJSON(node));

    // Plans that ran for this query without being part of its tree, listed among the nodes so a
    // reader walks one array, and described at the root so their roots can be told from the main one.
    auto sub_plan_entries = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & sub_plan : captured.sub_plans)
    {
        // Shaped like the root of the document, one level down: same keys, same meanings, for
        // the pipeline this subquery ran in. `ExecutionTimeNs` is that pipeline's own -- it ran
        // before the main one existed, so it is not part of the query's and the two do not sum.
        auto entry = std::make_unique<JSONBuilder::JSONMap>();
        entry->add("Id", sub_plan.subquery_id);
        entry->add("Kind", String(toString(sub_plan.kind)));
        entry->add("Root", sub_plan.root_id);

        /// Which steps use this subquery's result. Without it a reader sees a sub-plan that
        /// reads a large table and nothing saying what the query wanted it for. Absent when
        /// nothing in the document reads it -- a set used only by index analysis, say.
        if (const auto it = consumers_by_subquery.find(sub_plan.subquery_id); it != consumers_by_subquery.end())
        {
            auto consumers = std::make_unique<JSONBuilder::JSONArray>();
            for (const auto & node_id : it->second)
                consumers->add(node_id);
            entry->add("ConsumedBy", std::move(consumers));
        }

        if (sub_plan.execution_time_ns)
            entry->add("ExecutionTimeNs", *sub_plan.execution_time_ns);
        if (sub_plan.max_threads)
            entry->add("MaxThreads", *sub_plan.max_threads);
        sub_plan_entries->add(std::move(entry));

        for (const auto & node : sub_plan.nodes)
            nodes_array->add(capturedStepToJSON(node));
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
