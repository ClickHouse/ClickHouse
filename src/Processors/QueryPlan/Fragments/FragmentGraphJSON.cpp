#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/Fragments/PreparedFragmentGraph.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Common/Exception.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The exchange name of a send or receive step, or nullptr for any other step.
const String * tryGetExchangeName(const IQueryPlanStep & step)
{
    if (const auto * send = typeid_cast<const ShuffleSendStep *>(&step))
        return &send->getExchangeId();
    if (const auto * send = typeid_cast<const GatherSendStep *>(&step))
        return &send->getExchangeId();
    if (const auto * send = typeid_cast<const BroadcastSendStep *>(&step))
        return &send->getExchangeId();
    return PreparedFragmentGraph::tryGetReceiveExchangeId(step);
}

/// The fragment that produces the result of the query: the one that sends to no exchange of the graph.
const QueryFragment & getResultFragment(const FragmentGraph & graph)
{
    const QueryFragment * result = nullptr;
    for (const auto & fragment : graph.fragments)
    {
        const bool sends = std::ranges::any_of(graph.exchanges, [&](const auto & exchange) { return exchange.from == fragment.id; });
        if (sends)
            continue;
        if (result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragments {} and {} both send to no exchange", result->id, fragment.id);
        result = &fragment;
    }
    if (!result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Every fragment sends to an exchange");
    return *result;
}

/// The id of the exchange of every send and receive step of the fragments. The send step of the result
/// (`final_result`) sends to the client, not to an exchange of the graph, so it is not in the map.
MapStepToExchangeIds collectExchangeIds(const FragmentGraph & graph)
{
    std::unordered_map<std::string_view, UInt32> id_by_name;
    for (const auto & exchange : graph.exchanges)
        id_by_name.emplace(exchange.name, exchange.id);

    MapStepToExchangeIds exchange_ids;
    for (const auto & fragment : graph.fragments)
    {
        std::vector<const QueryPlan::Node *> stack{PreparedFragmentGraph::getPlan(fragment).getRootNode()};
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            if (const auto * name = tryGetExchangeName(*node->step))
                if (auto it = id_by_name.find(*name); it != id_by_name.end())
                    exchange_ids.emplace(node->step.get(), it->second);
            for (const auto * child : node->children)
                stack.push_back(child);
        }
    }
    return exchange_ids;
}

std::unique_ptr<JSONBuilder::JSONMap> explainFragment(const QueryFragment & fragment, const ExplainPlanOptions & options)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();
    map->add("Id", fragment.id);
    map->add("Stage", fragment.stage_name);
    map->add("Tasks", fragment.tasks);
    if (fragment.description)
        map->add("Description", *fragment.description);
    if (fragment.fallback_reason)
        map->add("Fallback Reason", *fragment.fallback_reason);
    const auto & plan = PreparedFragmentGraph::getPlan(fragment);
    map->add("Root", plan.getRootNode()->step->getUniqID());
    map->add("Steps", plan.explainPlanSteps(options));
    return map;
}

std::unique_ptr<JSONBuilder::JSONMap> explainExchange(const FragmentExchange & exchange, const ExplainPlanOptions & options)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();
    map->add("Id", exchange.id);
    map->add("Name", exchange.name);
    map->add("From", exchange.from);
    map->add("To", exchange.to);
    map->add("Kind", toString(exchange.kind));
    if (exchange.kind == FragmentExchange::Kind::Hash)
    {
        auto keys = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & key : exchange.keys)
            keys->add(key);
        map->add("Keys", std::move(keys));
    }
    if (exchange.merge_sort)
        map->add("Sort Description", explainSortDescription(*exchange.merge_sort));
    map->add("Mode", PreparedFragmentGraph::formatExchangeMode(exchange.mode));
    map->add("Source Buckets", exchange.source_buckets);
    map->add("Destination Buckets", exchange.destination_buckets);
    if (options.estimates && exchange.statistics.estimated_rows)
        map->add("Estimated Rows", *exchange.statistics.estimated_rows);
    return map;
}

}

std::unique_ptr<JSONBuilder::JSONMap> explainAsFragmentsJSON(const FragmentGraph & graph, const ExplainPlanOptions & options)
{
    if (graph.fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has no fragments");

    auto document = std::make_unique<JSONBuilder::JSONMap>();

    /// A plan that was not cut: the document of `json = 1`, and the reason it was not cut.
    const auto & first = graph.fragments.front();
    if (graph.fragments.size() == 1 && first.execution_mode == QueryFragment::ExecutionMode::SingleNode)
    {
        document->add("Plan", PreparedFragmentGraph::getPlan(first).explainPlan(options));
        if (first.fallback_reason)
            document->add("Fallback Reason", *first.fallback_reason);
        return document;
    }

    const auto exchange_ids = collectExchangeIds(graph);
    ExplainPlanOptions fragment_options = options;
    fragment_options.exchange_ids = &exchange_ids;

    /// `EXPLAIN` cuts the plan in place, so the plan of the whole query is gone; the document points to the fragment that
    /// produces the result instead.
    document->add("Result Fragment", getResultFragment(graph).id);

    auto fragments = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & fragment : graph.fragments)
        fragments->add(explainFragment(fragment, fragment_options));
    document->add("Fragments", std::move(fragments));

    auto exchanges = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & exchange : graph.exchanges)
        exchanges->add(explainExchange(exchange, options));
    document->add("Exchanges", std::move(exchanges));

    auto levels = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & level : computeLevels(graph))
    {
        auto ids = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto id : level)
            ids->add(id);
        levels->add(std::move(ids));
    }
    document->add("Levels", std::move(levels));

    return document;
}

}
