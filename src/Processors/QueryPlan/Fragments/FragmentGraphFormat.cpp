#include <Processors/QueryPlan/Fragments/FragmentGraphFormat.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

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
