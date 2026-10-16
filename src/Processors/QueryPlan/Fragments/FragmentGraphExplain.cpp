#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
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

String formatKind(const FragmentExchange & exchange)
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

    /// The plan was not cut: print it as `EXPLAIN` without `fragments` does.
    if (graph.fragments.size() == 1 && graph.exchanges.empty())
    {
        const auto & fragment = graph.fragments.front();
        getPlan(fragment).explainPlan(buf, options, 0, max_step_description_length);
        if (fragment.fallback_reason)
            buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
        return;
    }

    const auto print_order = getPrintOrder(graph);

    for (const auto * fragment : print_order)
    {
        buf << fmt::format("Query fragment {} ({}) · tasks={}\n", fragment->id, fragment->stage_name, fragment->tasks);
        getPlan(*fragment).explainPlan(buf, options, 0, max_step_description_length);
        buf << '\n';
    }

    if (!graph.exchanges.empty())
    {
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

    if (!graph.exchanges.empty())
    {
        buf << "Dependencies:\n";
        for (const auto * fragment : print_order)
        {
            for (const auto & exchange : graph.exchanges)
            {
                if (exchange.to == fragment->id)
                    buf << fmt::format("  F{} <- F{} ({})\n", fragment->id, exchange.from, exchange.name);
            }
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
