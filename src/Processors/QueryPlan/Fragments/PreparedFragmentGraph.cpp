#include <Processors/QueryPlan/Fragments/PreparedFragmentGraph.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ranges>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PreparedFragmentGraph::PreparedFragmentGraph(const FragmentGraph & graph_, const ExplainPlanOptions & options_)
    : graph(graph_)
    , options(options_)
    , single_node(graph.fragments.size() == 1 && graph.fragments.front().execution_mode == QueryFragment::ExecutionMode::SingleNode)
{
    if (graph.fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has no fragments");

    /// A plan that was not cut has its own naming scope, the one `QueryPlan::explainPlan` builds for it.
    if (single_node)
    {
        if (options.pretty)
            pretty_names = QueryPlanFormat::buildPrettyNamesPerPlan(getPlan(graph.fragments.front()));
        return;
    }

    print_order.reserve(graph.fragments.size());
    for (const auto & fragment : graph.fragments | std::views::reverse)
        print_order.push_back(&fragment);

    if (options.pretty)
    {
        std::vector<const QueryPlan *> plans;
        plans.reserve(print_order.size());
        for (const auto * fragment : print_order)
            plans.push_back(&getPlan(*fragment));
        pretty_names = QueryPlanFormat::buildPrettyNamesForFragments(plans, getFragmentInputs());
    }
}

String PreparedFragmentGraph::formatExchangeKind(const FragmentExchange & exchange) const
{
    /// The fragments share one column naming scope, so the map of any fragment names the keys.
    const PrettyColumnNameMap * column_names
        = options.pretty && !print_order.empty() ? &pretty_names.names.at(&getPlan(*print_order.front())).pretty_names : nullptr;

    switch (exchange.kind)
    {
        case FragmentExchange::Kind::Hash:
        {
            std::vector<String> keys;
            keys.reserve(exchange.keys.size());
            for (const auto & key : exchange.keys)
                keys.push_back(column_names ? QueryPlanFormat::formatColumnPretty(key, *column_names) : key);
            return fmt::format("hash({})", fmt::join(keys, ", "));
        }
        case FragmentExchange::Kind::Broadcast:
            return "broadcast";
        case FragmentExchange::Kind::Gather:
        {
            if (!exchange.merge_sort)
                return "gather";
            if (!column_names)
                return "gather sorted by " + dumpSortDescription(*exchange.merge_sort);
            WriteBufferFromOwnString out;
            ExplainFormatSettings settings{
                .out = out, .header_prefix = "", .detail_prefix = "", .pretty = true, .pretty_names = *column_names, .runtime_filter_names = {}};
            dumpSortDescription(*exchange.merge_sort, settings);
            return "gather sorted by " + out.str();
        }
        case FragmentExchange::Kind::Any:
            return "any";
        case FragmentExchange::Kind::SetValues:
            return "set values";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected kind of exchange {}", exchange.name);
}

std::string_view PreparedFragmentGraph::formatExchangeMode(ExchangeDescription::Kind mode)
{
    switch (mode)
    {
        case ExchangeDescription::Kind::Streaming:
            return "streaming";
        case ExchangeDescription::Kind::Persisted:
            return "persisted";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected exchange mode {}", static_cast<int>(mode));
}

const QueryPlan & PreparedFragmentGraph::getPlan(const QueryFragment & fragment)
{
    if (!fragment.plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to explain", fragment.id);
    return *fragment.plan;
}

const PrettyNames & PreparedFragmentGraph::getPrettyNames(const QueryPlan & plan) const
{
    static const PrettyNames no_names;
    if (!options.pretty)
        return no_names;
    auto it = pretty_names.names.find(&plan);
    if (it == pretty_names.names.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The plan to explain has no pretty names");
    return it->second;
}

const String * PreparedFragmentGraph::tryGetReceiveExchangeId(const IQueryPlanStep & step)
{
    if (const auto * receive = typeid_cast<const ShuffleReceiveStep *>(&step))
        return &receive->getExchangeId();
    if (const auto * receive = typeid_cast<const GatherReceiveStep *>(&step))
        return &receive->getExchangeId();
    if (const auto * receive = typeid_cast<const BroadcastReceiveStep *>(&step))
        return &receive->getExchangeId();
    return nullptr;
}

FragmentInputs PreparedFragmentGraph::getFragmentInputs() const
{
    std::unordered_map<UInt32, const QueryPlan *> plan_by_id;
    for (const auto & fragment : graph.fragments)
        plan_by_id.emplace(fragment.id, &getPlan(fragment));

    std::unordered_map<std::string_view, const QueryPlan *> sender_by_exchange;
    for (const auto & exchange : graph.exchanges)
        sender_by_exchange.emplace(exchange.name, plan_by_id.at(exchange.from));

    FragmentInputs inputs;
    for (const auto & fragment : graph.fragments)
    {
        std::vector<const QueryPlan::Node *> stack{getPlan(fragment).getRootNode()};
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            if (const auto * exchange_id = tryGetReceiveExchangeId(*node->step))
            {
                auto it = sender_by_exchange.find(*exchange_id);
                if (it == sender_by_exchange.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} receives from exchange {}, which is not in the graph",
                        fragment.id, *exchange_id);
                inputs.emplace(node->step.get(), it->second);
            }
            for (const auto * child : node->children)
                stack.push_back(child);
        }
    }
    return inputs;
}

void writeSingleNode(const PreparedFragmentGraph & prepared, WriteBuffer & buf, size_t max_step_description_length)
{
    chassert(prepared.single_node);
    const auto & fragment = prepared.graph.fragments.front();
    PreparedFragmentGraph::getPlan(fragment).explainPlan(
        buf, prepared.options, 0, max_step_description_length,
        prepared.options.pretty ? &prepared.pretty_names : nullptr);
    if (fragment.fallback_reason)
        buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
}

void writeExchangesSection(const PreparedFragmentGraph & prepared, WriteBuffer & buf)
{
    if (prepared.graph.exchanges.empty())
        return;

    buf << "Exchanges:\n";
    for (const auto & exchange : prepared.graph.exchanges)
    {
        buf << fmt::format("  {}: {}, {}, F{} -> F{}",
            exchange.name,
            prepared.formatExchangeKind(exchange),
            PreparedFragmentGraph::formatExchangeMode(exchange.mode),
            exchange.from,
            exchange.to);
        if (exchange.source_buckets || exchange.destination_buckets)
            buf << fmt::format(", buckets {} -> {}", exchange.source_buckets, exchange.destination_buckets);
        buf << '\n';
    }
}

void writeLevelsSection(const FragmentGraph & graph, WriteBuffer & buf)
{
    const auto levels = computeLevels(graph);
    buf << "Levels: ";
    for (size_t i = 0; i < levels.size(); ++i)
    {
        if (i != 0)
            buf << " -> ";
        std::vector<String> names;
        names.reserve(levels[i].size());
        for (UInt32 id : levels[i])
            names.push_back(fmt::format("F{}", id));
        buf << fmt::format("[{}]", fmt::join(names, ", "));
    }
    buf << '\n';
}

}
