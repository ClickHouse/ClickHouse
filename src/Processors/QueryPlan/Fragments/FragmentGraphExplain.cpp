#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

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

std::string_view formatRole(QueryFragment::Role role)
{
    switch (role)
    {
        case QueryFragment::Role::Initiator:
            return "initiator";
        case QueryFragment::Role::Worker:
            return "worker";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected role of a query fragment");
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
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected kind of exchange {}", exchange.name);
}

std::string_view formatMode(FragmentExchange::Mode mode)
{
    switch (mode)
    {
        case FragmentExchange::Mode::Pipelined:
            return "pipelined";
        case FragmentExchange::Mode::Materialised:
            return "materialised";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected mode of exchange");
}

/// The initiator first, then the other fragments by descending id.
std::vector<const QueryFragment *> getPrintOrder(const FragmentGraph & graph)
{
    std::vector<const QueryFragment *> order;
    order.reserve(graph.fragments.size());
    for (const auto & fragment : graph.fragments)
        order.push_back(&fragment);

    std::sort(order.begin(), order.end(), [](const QueryFragment * lhs, const QueryFragment * rhs)
    {
        const bool lhs_is_initiator = lhs->role == QueryFragment::Role::Initiator;
        const bool rhs_is_initiator = rhs->role == QueryFragment::Role::Initiator;
        if (lhs_is_initiator != rhs_is_initiator)
            return lhs_is_initiator;
        return lhs->id > rhs->id;
    });
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
        getPlan(graph.fragments.front()).explainPlan(buf, options, 0, max_step_description_length);
        if (graph.fallback_reason)
            buf << "\nNot distributed: " << *graph.fallback_reason << '\n';
        return;
    }

    if (graph.fallback_reason)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A fragment graph with {} fragments has a fallback reason", graph.fragments.size());

    const auto print_order = getPrintOrder(graph);

    for (const auto * fragment : print_order)
    {
        buf << fmt::format("Fragment {} ({}, {}, tasks={})\n", fragment->id, fragment->stage_name, formatRole(fragment->role), fragment->tasks);
        getPlan(*fragment).explainPlan(buf, options, 0, max_step_description_length);
        buf << '\n';
    }

    if (!graph.exchanges.empty())
    {
        buf << "Exchanges:\n";
        for (const auto & exchange : graph.exchanges)
        {
            buf << fmt::format("  {}: {}, {}, Fragment {} -> Fragment {}",
                exchange.name, formatKind(exchange), formatMode(exchange.mode), exchange.from, exchange.to);
            if (exchange.source_buckets || exchange.destination_buckets)
                buf << fmt::format(", buckets {} -> {}", exchange.source_buckets, exchange.destination_buckets);
            buf << '\n';
        }
    }

    bool has_dependencies = false;
    for (const auto & fragment : graph.fragments)
        has_dependencies |= !fragment.depends_on.empty();

    if (has_dependencies)
    {
        buf << "Dependencies:\n";
        for (const auto * fragment : print_order)
        {
            for (const auto & dependency : fragment->depends_on)
            {
                buf << fmt::format("  Fragment {} <- Fragment {} ({}, ", fragment->id, dependency.producer, graph.getExchange(dependency.exchange).name);
                switch (dependency.type)
                {
                    case FragmentDependency::Type::Streaming:
                        buf << "streaming";
                        break;
                    case FragmentDependency::Type::Blocking:
                        buf << "blocking: " << dependency.reason;
                        break;
                }
                buf << ")\n";
            }
        }
    }

    buf << "Levels: ";
    for (size_t i = 0; i < graph.levels.size(); ++i)
    {
        if (i != 0)
            buf << " -> ";
        buf << fmt::format("[{}]", fmt::join(graph.levels[i], ", "));
    }
    buf << '\n';
}

}
