#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
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

namespace
{

const QueryPlan & getPlan(const QueryFragment & fragment)
{
    if (!fragment.plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to explain", fragment.id);
    return *fragment.plan;
}

const String * tryGetReceiveExchangeId(const IQueryPlanStep & step)
{
    if (const auto * receive = typeid_cast<const ShuffleReceiveStep *>(&step))
        return &receive->getExchangeId();
    if (const auto * receive = typeid_cast<const GatherReceiveStep *>(&step))
        return &receive->getExchangeId();
    if (const auto * receive = typeid_cast<const BroadcastReceiveStep *>(&step))
        return &receive->getExchangeId();
    return nullptr;
}

/// Every receive step of a fragment with the plan of the fragment that sends to it.
FragmentInputs getFragmentInputs(const FragmentGraph & graph)
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

/// With `pretty`, the keys and the sort columns are named as in the fragments.
String formatKind(const FragmentExchange & exchange, const PrettyColumnNameMap * pretty_names)
{
    switch (exchange.kind)
    {
        case FragmentExchange::Kind::Hash:
        {
            std::vector<String> keys;
            keys.reserve(exchange.keys.size());
            for (const auto & key : exchange.keys)
                keys.push_back(pretty_names ? QueryPlanFormat::formatColumnPretty(key, *pretty_names) : key);
            return fmt::format("hash({})", fmt::join(keys, ", "));
        }
        case FragmentExchange::Kind::Broadcast:
            return "broadcast";
        case FragmentExchange::Kind::Gather:
        {
            if (!exchange.merge_sort)
                return "gather";
            if (!pretty_names)
                return "gather sorted by " + dumpSortDescription(*exchange.merge_sort);
            WriteBufferFromOwnString out;
            ExplainFormatSettings settings{
                .out = out, .header_prefix = "", .detail_prefix = "", .pretty = true, .pretty_names = *pretty_names, .runtime_filter_names = {}};
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

std::string_view formatMode(ExchangeDescription::Kind mode)
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

/// By descending id. The cutter numbers the stages in post-order, so a fragment comes before the fragments it reads,
/// and the fragment that produces the result (`main`) comes first.
std::vector<const QueryFragment *> getPrintOrder(const FragmentGraph & graph)
{
    std::vector<const QueryFragment *> order;
    order.reserve(graph.fragments.size());
    for (const auto & fragment : graph.fragments | std::views::reverse)
        order.push_back(&fragment);
    return order;
}

}

void explainFragmentGraph(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length)
{
    if (graph.fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has no fragments");

    /// The plan was not cut: print it as `EXPLAIN` without `fragments` does. A cut plan always gets fragment headers,
    /// even when it has a single fragment.
    if (graph.fragments.size() == 1 && graph.fragments.front().execution_mode == QueryFragment::ExecutionMode::SingleNode)
    {
        const auto & fragment = graph.fragments.front();
        getPlan(fragment).explainPlan(buf, options, 0, max_step_description_length);
        if (fragment.fallback_reason)
            buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
        return;
    }

    const auto print_order = getPrintOrder(graph);

    /// One registry for all fragments, so that a column, a runtime filter or a set has the same name in every fragment
    /// and in the `Exchanges` section. The names are built over the fragments joined at their exchanges, so they are
    /// the names plain `EXPLAIN` gives the plan before the cut.
    PrettyNamesPerPlan pretty_names;
    const PrettyColumnNameMap * column_names = nullptr;
    if (options.pretty)
    {
        std::vector<const QueryPlan *> plans;
        plans.reserve(print_order.size());
        for (const auto * fragment : print_order)
            plans.push_back(&getPlan(*fragment));
        pretty_names = QueryPlanFormat::buildPrettyNamesForFragments(plans, getFragmentInputs(graph));
        column_names = &pretty_names.names.at(plans.front()).pretty_names;
    }

    for (const auto * fragment : print_order)
    {
        buf << fmt::format("Query fragment {} ({}) · tasks={}\n", fragment->id, fragment->stage_name, fragment->tasks);
        getPlan(*fragment).explainPlan(buf, options, 0, max_step_description_length, options.pretty ? &pretty_names : nullptr);
        buf << '\n';
    }

    if (!graph.exchanges.empty())
    {
        buf << "Exchanges:\n";
        for (const auto & exchange : graph.exchanges)
        {
            buf << fmt::format("  {}: {}, {}, F{} -> F{}",
                exchange.name, formatKind(exchange, column_names), formatMode(exchange.mode), exchange.from, exchange.to);
            if (exchange.source_buckets || exchange.destination_buckets)
                buf << fmt::format(", buckets {} -> {}", exchange.source_buckets, exchange.destination_buckets);
            buf << '\n';
        }
    }

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
