#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/PlanIndexStats.h>
#include <Processors/QueryPlan/QueryPlanShapeHash.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatisticsJSONPrinter.h>
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

    if (step.shape_hash)
        map->add("ShapeHash", formatQueryPlanShapeHash(*step.shape_hash));

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

    auto children = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & child : step.children)
        children->add(child);
    map->add("Children", std::move(children));

    return map;
}

}

JSONBuilder::ItemPtr capturedPlanToJSON(const CapturedPlan & captured)
{
    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & node : captured.nodes)
        nodes_array->add(capturedStepToJSON(node));

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

    result->add("Nodes", std::move(nodes_array));

    return result;
}

}
