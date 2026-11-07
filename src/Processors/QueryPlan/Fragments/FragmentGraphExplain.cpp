#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FragmentGraphExplain::FragmentGraphExplain(
    const FragmentGraph & graph_, const ExplainPlanOptions & options_, size_t max_step_description_length_)
    : graph(graph_)
    , options(options_)
    , max_step_description_length(max_step_description_length_)
    , single_node(graph.fragments.size() == 1 && graph.fragments.front().execution_mode == QueryFragment::ExecutionMode::SingleNode)
{
    if (graph.fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has no fragments");

    /// A plan that was not cut is printed by `QueryPlan::explainPlan` alone, which builds its own names.
    if (single_node)
        return;

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

void FragmentGraphExplain::explain(WriteBuffer & buf) const
{
    if (single_node)
    {
        explainSingleNode(buf);
        return;
    }

    explainFragments(buf);
    explainExchanges(buf);
    explainLevels(buf);
}

void FragmentGraphExplain::explainSingleNode(WriteBuffer & buf) const
{
    const auto & fragment = graph.fragments.front();
    getPlan(fragment).explainPlan(buf, options, 0, max_step_description_length);
    if (fragment.fallback_reason)
        buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
}

void FragmentGraphExplain::explainFragments(WriteBuffer & buf) const
{
    for (const auto * fragment : print_order)
    {
        buf << fmt::format("Query fragment {} ({}) · tasks={}\n", fragment->id, fragment->stage_name, fragment->tasks);
        getPlan(*fragment).explainPlan(buf, options, 0, max_step_description_length, options.pretty ? &pretty_names : nullptr);
        buf << '\n';
    }
}

void FragmentGraphExplain::explainExchanges(WriteBuffer & buf) const
{
    if (graph.exchanges.empty())
        return;

    buf << "Exchanges:\n";
    for (const auto & exchange : graph.exchanges)
    {
        buf << fmt::format("  {}: {}, {}, F{} -> F{}",
            exchange.name, formatKind(exchange), formatMode(exchange.mode), exchange.from, exchange.to);
        if (exchange.source_buckets || exchange.destination_buckets)
            buf << fmt::format(", buckets {} -> {}", exchange.source_buckets, exchange.destination_buckets);
        buf << '\n';
    }
}

void FragmentGraphExplain::explainLevels(WriteBuffer & buf) const
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

String FragmentGraphExplain::formatKind(const FragmentExchange & exchange) const
{
    /// The fragments share one column naming scope, so the map of any fragment names the keys.
    const PrettyColumnNameMap * column_names
        = options.pretty ? &pretty_names.names.at(&getPlan(*print_order.front())).pretty_names : nullptr;

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

std::string_view FragmentGraphExplain::formatMode(ExchangeDescription::Kind mode)
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

const QueryPlan & FragmentGraphExplain::getPlan(const QueryFragment & fragment)
{
    if (!fragment.plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to explain", fragment.id);
    return *fragment.plan;
}

std::optional<std::string_view> FragmentGraphExplain::tryGetReceiveExchangeId(const IQueryPlanStep & step)
{
    if (const auto * receive = typeid_cast<const ShuffleReceiveStep *>(&step))
        return receive->getExchangeId();
    if (const auto * receive = typeid_cast<const GatherReceiveStep *>(&step))
        return receive->getExchangeId();
    if (const auto * receive = typeid_cast<const BroadcastReceiveStep *>(&step))
        return receive->getExchangeId();
    return std::nullopt;
}

FragmentInputs FragmentGraphExplain::getFragmentInputs() const
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
            if (const auto exchange_id = tryGetReceiveExchangeId(*node->step))
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

}
