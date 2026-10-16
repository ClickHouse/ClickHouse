#include <Processors/QueryPlan/Fragments/FragmentGraph.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cmath>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr std::string_view main_stage_name = "main";

const DistributedQueryStage & findStage(const DistributedQueryPlan & distributed_plan, const String & stage_name)
{
    auto it = distributed_plan.stages.find(stage_name);
    if (it == distributed_plan.stages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stage {} is referenced by a dependency, but does not exist", stage_name);
    return it->second;
}

const ExchangeDescription & findDescription(const DistributedQueryPlan & distributed_plan, const String & exchange_name)
{
    auto it = distributed_plan.exchange_descriptions.find(exchange_name);
    if (it == distributed_plan.exchange_descriptions.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} has no description", exchange_name);
    return it->second;
}

bool isExchangeEndpointStep(const IQueryPlanStep & step)
{
    return typeid_cast<const ShuffleSendStep *>(&step) || typeid_cast<const GatherSendStep *>(&step)
        || typeid_cast<const BroadcastSendStep *>(&step) || typeid_cast<const ShuffleReceiveStep *>(&step)
        || typeid_cast<const GatherReceiveStep *>(&step) || typeid_cast<const BroadcastReceiveStep *>(&step);
}

/// A fragment is a stage of the executor, so it always has a plan.
void checkNotEmpty(const QueryPlan & plan, const String & stage_name)
{
    if (!plan.getRootNode())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The plan of stage {} is empty", stage_name);
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

/// Checks that `plan` has exactly one receive step of `exchange_name`: a dependency of the cut plan without one is
/// an inconsistent cut.
void checkReceiveStep(const QueryPlan & plan, const String & stage_name, const String & exchange_name)
{
    size_t receive_steps = 0;
    std::vector<const QueryPlan::Node *> stack{plan.getRootNode()};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (const auto * exchange_id = tryGetReceiveExchangeId(*node->step); exchange_id && *exchange_id == exchange_name)
            ++receive_steps;
        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }

    if (receive_steps == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Stage {} depends on exchange {}, but has no receive step for it", stage_name, exchange_name);
    if (receive_steps > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Stage {} has more than one receive step for exchange {}", stage_name, exchange_name);
}

/// The estimated number of rows a producer fragment sends: the estimate of its root node,
/// or of the input of its send step, because the send step is added by the cutter without an estimate.
std::optional<UInt64> getEstimatedRows(const QueryPlan & plan)
{
    const auto * node = plan.getRootNode();
    if (!node)
        return std::nullopt;
    if (!node->cost_estimation && isExchangeEndpointStep(*node->step) && node->children.size() == 1)
        node = node->children.front();
    if (!node->cost_estimation)
        return std::nullopt;
    return static_cast<UInt64>(std::llround(std::max(0.0, node->cost_estimation->rows)));
}

FragmentExchange::Kind getExchangeKind(const ExchangeDescription & description)
{
    if (description.exchange_step_name == "ShuffleExchange" || description.exchange_step_name == "ScatterExchange")
        return description.keys.empty() ? FragmentExchange::Kind::Any : FragmentExchange::Kind::Hash;
    if (description.exchange_step_name == "BroadcastExchange")
        return FragmentExchange::Kind::Broadcast;
    if (description.exchange_step_name == "GatherExchange")
        return FragmentExchange::Kind::Gather;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected exchange step '{}' of exchange {}",
        description.exchange_step_name, description.name);
}

FragmentExchange::Mode getExchangeMode(const ExchangeDescription & description)
{
    switch (description.kind)
    {
        case ExchangeDescription::Kind::Streaming:
            return FragmentExchange::Mode::Pipelined;
        case ExchangeDescription::Kind::Persisted:
            return FragmentExchange::Mode::Materialised;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected kind of exchange {}", description.name);
}

String formatExchangeKind(const FragmentExchange & exchange)
{
    switch (exchange.kind)
    {
        case FragmentExchange::Kind::Hash:
            return fmt::format("hash({})", fmt::join(exchange.keys, ", "));
        case FragmentExchange::Kind::Broadcast:
            return "broadcast";
        case FragmentExchange::Kind::Gather:
            if (exchange.merge_sort)
                return fmt::format("gather(merge: {})", dumpSortDescription(*exchange.merge_sort));
            return "gather";
        case FragmentExchange::Kind::Any:
            return "any";
        case FragmentExchange::Kind::SetValues:
            return "set values";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected kind of exchange {}", exchange.id);
}

}

const QueryFragment & FragmentGraph::getFragment(UInt32 id) const
{
    auto it = std::lower_bound(fragments.begin(), fragments.end(), id, [](const QueryFragment & f, UInt32 value) { return f.id < value; });
    if (it == fragments.end() || it->id != id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} is not found in the fragment graph", id);
    return *it;
}

const FragmentExchange & FragmentGraph::getExchange(UInt32 id) const
{
    auto it = std::lower_bound(exchanges.begin(), exchanges.end(), id, [](const FragmentExchange & e, UInt32 value) { return e.id < value; });
    if (it == exchanges.end() || it->id != id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} is not found in the fragment graph", id);
    return *it;
}

namespace
{

/// The indexes of the fragments of a valid graph in a topological order, producers first, and the producers of each.
struct TopologicalOrder
{
    std::vector<size_t> order;
    /// Fragment index -> the indexes of the fragments it reads from.
    std::vector<std::vector<size_t>> producers;
};

/// Validates the graph (see `validateFragmentGraph`) and sorts it. Ready fragments are taken in the order of ids
/// for determinism. The ids assigned by the cutter already ascend from producers to consumers; the sort does not
/// rely on that.
TopologicalOrder sortTopologically(const FragmentGraph & graph)
{
    const size_t num_fragments = graph.fragments.size();

    std::unordered_map<UInt32, size_t> fragment_index_by_id;
    for (size_t i = 0; i < num_fragments; ++i)
    {
        if (!fragment_index_by_id.emplace(graph.fragments[i].id, i).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate fragment {} in the fragment graph", graph.fragments[i].id);
    }

    std::unordered_set<UInt32> exchange_ids;
    for (const auto & exchange : graph.exchanges)
    {
        if (!exchange_ids.insert(exchange.id).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate exchange {} in the fragment graph", exchange.id);
    }

    TopologicalOrder result;
    result.producers.resize(num_fragments);
    std::vector<size_t> in_degree(num_fragments, 0);
    std::vector<std::vector<size_t>> consumers(num_fragments);
    for (const auto & exchange : graph.exchanges)
    {
        auto producer_it = fragment_index_by_id.find(exchange.from);
        if (producer_it == fragment_index_by_id.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} comes from unknown fragment {}", exchange.name, exchange.from);
        auto consumer_it = fragment_index_by_id.find(exchange.to);
        if (consumer_it == fragment_index_by_id.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} goes to unknown fragment {}", exchange.name, exchange.to);
        consumers[producer_it->second].push_back(consumer_it->second);
        result.producers[consumer_it->second].push_back(producer_it->second);
        ++in_degree[consumer_it->second];
    }

    std::priority_queue<std::pair<UInt32, size_t>, std::vector<std::pair<UInt32, size_t>>, std::greater<>> ready;
    for (size_t i = 0; i < num_fragments; ++i)
        if (in_degree[i] == 0)
            ready.emplace(graph.fragments[i].id, i);

    result.order.reserve(num_fragments);
    while (!ready.empty())
    {
        size_t i = ready.top().second;
        ready.pop();
        result.order.push_back(i);
        for (size_t consumer : consumers[i])
            if (--in_degree[consumer] == 0)
                ready.emplace(graph.fragments[consumer].id, consumer);
    }

    if (result.order.size() != num_fragments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has a cycle");

    /// The root is the fragment whose output nobody consumes: the result of the query.
    size_t num_roots = 0;
    for (size_t i = 0; i < num_fragments; ++i)
        if (consumers[i].empty())
            ++num_roots;
    if (num_roots != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has {} root fragments, expected one", num_roots);

    return result;
}

}

void validateFragmentGraph(const FragmentGraph & graph)
{
    sortTopologically(graph);
}

std::vector<std::vector<UInt32>> computeLevels(const FragmentGraph & graph)
{
    const auto sorted = sortTopologically(graph);

    /// A fragment is one level above its deepest producer.
    std::vector<size_t> level_of(graph.fragments.size(), 0);
    size_t max_level = 0;
    for (size_t i : sorted.order)
    {
        for (size_t producer : sorted.producers[i])
            level_of[i] = std::max(level_of[i], level_of[producer] + 1);
        max_level = std::max(max_level, level_of[i]);
    }

    std::vector<std::vector<UInt32>> levels(max_level + 1);
    for (size_t i = 0; i < graph.fragments.size(); ++i)
        levels[level_of[i]].push_back(graph.fragments[i].id);
    for (auto & level : levels)
        std::sort(level.begin(), level.end());
    return levels;
}

FragmentGraph buildFragmentGraph(const DistributedQueryPlan & distributed_plan)
{
    FragmentGraph graph;

    /// The graph is the data flow of the query. The merge stages of runtime filters (`filter_only`, added by
    /// `wireRuntimeFilterExchangeTopology`) produce no query data and are not cut from the plan, so they and the
    /// dependencies on either side of them are left out. The steps that build and apply the filters stay in the plans
    /// of the data fragments.
    for (const auto & [stage_name, stage] : distributed_plan.stages)
    {
        if (stage.filter_only)
            continue;

        QueryFragment fragment;
        fragment.id = stage.id;
        fragment.stage_name = stage_name;
        fragment.tasks = stage.tasks.size();
        checkNotEmpty(stage.query_plan_fragment, stage_name);
        fragment.plan = &stage.query_plan_fragment;
        graph.fragments.push_back(std::move(fragment));
    }
    std::sort(graph.fragments.begin(), graph.fragments.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });
    for (size_t i = 1; i < graph.fragments.size(); ++i)
        if (graph.fragments[i - 1].id == graph.fragments[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stages {} and {} have the same id {}",
                graph.fragments[i - 1].stage_name, graph.fragments[i].stage_name, graph.fragments[i].id);

    auto find_fragment = [&](UInt32 id) -> const QueryFragment &
    {
        auto it = std::lower_bound(graph.fragments.begin(), graph.fragments.end(), id,
            [](const QueryFragment & f, UInt32 value) { return f.id < value; });
        if (it == graph.fragments.end() || it->id != id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stage of fragment {} is referenced by a dependency, but does not exist", id);
        return *it;
    };

    for (const auto & [consumer_stage, producers] : distributed_plan.stage_depends_on)
    {
        const auto & consumer_stage_description = findStage(distributed_plan, consumer_stage);
        if (consumer_stage_description.filter_only)
            continue;
        UInt32 consumer_id = consumer_stage_description.id;
        for (const auto & [producer_stage, exchange_name] : producers)
        {
            const auto & producer_stage_description = findStage(distributed_plan, producer_stage);
            if (producer_stage_description.filter_only)
                continue;
            UInt32 producer_id = producer_stage_description.id;
            const auto & description = findDescription(distributed_plan, exchange_name);

            FragmentExchange exchange;
            exchange.id = description.id;
            exchange.name = description.name;
            exchange.from = producer_id;
            exchange.to = consumer_id;
            exchange.kind = getExchangeKind(description);
            if (exchange.kind == FragmentExchange::Kind::Hash)
                exchange.keys = description.keys;
            if (exchange.kind == FragmentExchange::Kind::Gather)
                exchange.merge_sort = description.sort_description;
            exchange.mode = getExchangeMode(description);
            exchange.source_buckets = description.source_bucket_count;
            exchange.destination_buckets = description.destination_bucket_count;
            exchange.statistics.estimated_rows = getEstimatedRows(*find_fragment(producer_id).plan);

            checkReceiveStep(*find_fragment(consumer_id).plan, consumer_stage, exchange_name);

            graph.exchanges.push_back(std::move(exchange));
        }
    }

    std::sort(graph.exchanges.begin(), graph.exchanges.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });
    for (size_t i = 1; i < graph.exchanges.size(); ++i)
        if (graph.exchanges[i - 1].id == graph.exchanges[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchanges {} and {} have the same id {}",
                graph.exchanges[i - 1].name, graph.exchanges[i].name, graph.exchanges[i].id);

    validateFragmentGraph(graph);
    return graph;
}

FragmentGraph buildSingleFragmentGraph(const QueryPlan & plan, std::optional<String> fallback_reason)
{
    FragmentGraph graph;

    QueryFragment fragment;
    fragment.id = 0;
    fragment.stage_name = String(main_stage_name);
    fragment.location = QueryFragment::ExecutionLocation::SingleNode;
    fragment.fallback_reason = std::move(fallback_reason);
    fragment.tasks = 1;
    checkNotEmpty(plan, fragment.stage_name);
    fragment.plan = &plan;
    graph.fragments.push_back(std::move(fragment));
    return graph;
}

String dumpFragmentGraph(const FragmentGraph & graph)
{
    String result;
    auto out = std::back_inserter(result);

    size_t stage_name_width = 0;
    for (const auto & fragment : graph.fragments)
        stage_name_width = std::max(stage_name_width, fragment.stage_name.size());

    fmt::format_to(out, "query fragments: {}\n", graph.fragments.size());
    for (const auto & fragment : graph.fragments)
    {
        fmt::format_to(out, "  F{} {:<{}}  {:<11}  tasks={}",
            fragment.id, fragment.stage_name, stage_name_width,
            fragment.location == QueryFragment::ExecutionLocation::Distributed ? "distributed" : "single node",
            fragment.tasks);

        bool first_input = true;
        for (const auto & exchange : graph.exchanges)
        {
            if (exchange.to != fragment.id)
                continue;
            fmt::format_to(out, "{}F{} via {}", first_input ? "  needs " : ", ", exchange.from, exchange.name);
            first_input = false;
        }
        if (fragment.fallback_reason)
            fmt::format_to(out, "  not distributed: {}", *fragment.fallback_reason);
        if (fragment.description)
            fmt::format_to(out, "  description: {}", *fragment.description);
        result += '\n';
    }

    struct ExchangeColumns
    {
        String name;
        String edge;
        String kind;
        String mode;
        String buckets;
    };
    std::vector<ExchangeColumns> rows;
    size_t name_width = 0;
    size_t edge_width = 0;
    size_t kind_width = 0;
    size_t mode_width = 0;
    for (const auto & exchange : graph.exchanges)
    {
        ExchangeColumns row
        {
            .name = exchange.name,
            .edge = fmt::format("F{} -> F{}", exchange.from, exchange.to),
            .kind = formatExchangeKind(exchange),
            .mode = exchange.mode == FragmentExchange::Mode::Pipelined ? "pipelined" : "materialised",
            .buckets = fmt::format("{} -> {}", exchange.source_buckets, exchange.destination_buckets),
        };
        name_width = std::max(name_width, row.name.size());
        edge_width = std::max(edge_width, row.edge.size());
        kind_width = std::max(kind_width, row.kind.size());
        mode_width = std::max(mode_width, row.mode.size());
        rows.push_back(std::move(row));
    }

    fmt::format_to(out, "exchanges: {}\n", graph.exchanges.size());
    for (size_t i = 0; i < rows.size(); ++i)
    {
        const auto & row = rows[i];
        fmt::format_to(out, "  {:<{}}  {:<{}}  {:<{}}  {:<{}}  {}",
            row.name, name_width, row.edge, edge_width, row.kind, kind_width, row.mode, mode_width, row.buckets);
        if (graph.exchanges[i].statistics.estimated_rows)
            fmt::format_to(out, "  ~{} rows", *graph.exchanges[i].statistics.estimated_rows);
        result += '\n';
    }

    result += "levels:";
    for (const auto & level : computeLevels(graph))
    {
        std::vector<String> names;
        names.reserve(level.size());
        for (UInt32 id : level)
            names.push_back(fmt::format("F{}", id));
        fmt::format_to(out, " [{}]", fmt::join(names, ", "));
    }
    result += '\n';

    return result;
}

}
