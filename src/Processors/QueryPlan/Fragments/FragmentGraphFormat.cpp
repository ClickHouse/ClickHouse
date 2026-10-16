#include <Processors/QueryPlan/Fragments/FragmentGraphFormat.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

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

String formatExchangeKindRaw(const FragmentExchange & exchange)
{
    switch (exchange.kind)
    {
        case FragmentExchange::Kind::Hash:
            return fmt::format("hash({})", fmt::join(exchange.keys, ", "));
        case FragmentExchange::Kind::Broadcast:
            return "broadcast";
        case FragmentExchange::Kind::Gather:
            if (exchange.merge_sort)
                return "gather sorted by " + dumpSortDescription(*exchange.merge_sort);
            return "gather";
        case FragmentExchange::Kind::Any:
            return "any";
        case FragmentExchange::Kind::SetValues:
            return "set values";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected kind of exchange {}", exchange.name);
}

}

String formatExchangeKind(const FragmentExchange & exchange, bool pretty)
{
    auto result = formatExchangeKindRaw(exchange);
    return pretty ? QueryPlanFormat::trimColumnIdentifier(result) : result;
}

std::string_view formatExchangeMode(ExchangeDescription::Kind mode)
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

const String * tryGetReceiveExchangeId(const IQueryPlanStep & step)
{
    if (const auto * shuffle = typeid_cast<const ShuffleReceiveStep *>(&step))
        return &shuffle->getExchangeId();
    if (const auto * gather = typeid_cast<const GatherReceiveStep *>(&step))
        return &gather->getExchangeId();
    if (const auto * broadcast = typeid_cast<const BroadcastReceiveStep *>(&step))
        return &broadcast->getExchangeId();
    return nullptr;
}

std::vector<const QueryFragment *> getFragmentPrintOrder(const FragmentGraph & graph)
{
    std::vector<const QueryFragment *> order;
    order.reserve(graph.fragments.size());
    for (const auto & fragment : graph.fragments | std::views::reverse)
        order.push_back(&fragment);
    return order;
}

bool tryExplainSingleFragment(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length)
{
    if (graph.fragments.size() != 1 || !graph.exchanges.empty())
        return false;

    const auto & fragment = graph.fragments.front();
    if (!fragment.plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to explain", fragment.id);
    fragment.plan->explainPlan(buf, options, 0, max_step_description_length);
    if (fragment.fallback_reason)
        buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
    return true;
}

void writeExchangesSection(const FragmentGraph & graph, WriteBuffer & buf, bool pretty)
{
    if (graph.exchanges.empty())
        return;

    buf << "Exchanges:\n";
    for (const auto & exchange : graph.exchanges)
    {
        buf << fmt::format("  {}: {}, {}, F{} -> F{}",
            exchange.name, formatExchangeKind(exchange, pretty), formatExchangeMode(exchange.mode), exchange.from, exchange.to);
        if (exchange.source_buckets || exchange.destination_buckets)
            buf << fmt::format(", buckets {} -> {}", exchange.source_buckets, exchange.destination_buckets);
        buf << '\n';
    }
}

void writeDependenciesSection(const FragmentGraph & graph, WriteBuffer & buf)
{
    if (graph.exchanges.empty())
        return;

    buf << "Dependencies:\n";
    for (const auto * fragment : getFragmentPrintOrder(graph))
    {
        for (const auto & exchange : graph.exchanges)
        {
            if (exchange.to == fragment->id)
                buf << fmt::format("  F{} <- F{} ({})\n", fragment->id, exchange.from, exchange.name);
        }
    }
}

}
