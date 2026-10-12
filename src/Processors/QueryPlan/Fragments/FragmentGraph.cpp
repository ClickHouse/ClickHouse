#include <Processors/QueryPlan/Fragments/FragmentGraph.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cmath>
#include <queue>
#include <tuple>
#include <unordered_map>

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

bool isPassThroughStep(const IQueryPlanStep & step)
{
    return typeid_cast<const ExpressionStep *>(&step) || typeid_cast<const FilterStep *>(&step);
}

std::vector<String> buildSummary(const QueryPlan & plan)
{
    std::vector<String> summary;
    const auto * root = plan.getRootNode();
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A plan fragment of a distributed query plan is empty");

    std::vector<const QueryPlan::Node *> stack{root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (!isPassThroughStep(*node->step) && !isExchangeEndpointStep(*node->step))
            summary.push_back(node->step->getName());

        /// Push in reverse order so that the first child is visited first.
        for (auto it = node->children.rbegin(); it != node->children.rend(); ++it)
            stack.push_back(*it);
    }
    return summary;
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

/// Finds the receive step of `exchange_name` in `plan` and describes the step which consumes its output.
ConsumerStepInfo findConsumerStep(const QueryPlan & plan, const String & stage_name, const String & exchange_name)
{
    const auto * root = plan.getRootNode();
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The plan fragment of stage {} is empty", stage_name);

    /// Node -> (parent, index of the node among the children of the parent).
    std::unordered_map<const QueryPlan::Node *, std::pair<const QueryPlan::Node *, size_t>> parents;
    const QueryPlan::Node * receive_node = nullptr;

    std::vector<const QueryPlan::Node *> stack{root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (const auto * exchange_id = tryGetReceiveExchangeId(*node->step); exchange_id && *exchange_id == exchange_name)
        {
            if (receive_node)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Stage {} has more than one receive step for exchange {}", stage_name, exchange_name);
            receive_node = node;
        }

        for (size_t i = 0; i < node->children.size(); ++i)
        {
            parents[node->children[i]] = {node, i};
            stack.push_back(node->children[i]);
        }
    }

    if (!receive_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Stage {} depends on exchange {}, but has no receive step for it", stage_name, exchange_name);

    const QueryPlan::Node * current = receive_node;
    while (true)
    {
        auto it = parents.find(current);
        if (it == parents.end())
            return {}; /// No consumer step: the receive step feeds the result of the fragment.

        const auto [parent, child_index] = it->second;
        const auto & step = *parent->step;
        if (isPassThroughStep(step))
        {
            current = parent;
            continue;
        }

        ConsumerStepInfo info;
        info.step_name = step.getName();
        if (typeid_cast<const JoinStepLogical *>(&step))
        {
            info.kind = ConsumerStepInfo::Kind::Join;
            info.receive_feeds_build_side = child_index == 1;
        }
        else if (const auto * join = typeid_cast<const JoinStep *>(&step))
        {
            info.kind = ConsumerStepInfo::Kind::Join;
            const size_t build_side_index = join->swap_streams ? 0 : 1;
            info.receive_feeds_build_side = child_index == build_side_index;
        }
        else if (typeid_cast<const MergingAggregatedStep *>(&step))
        {
            info.kind = ConsumerStepInfo::Kind::MergingAggregated;
        }
        else if (const auto * sorting = typeid_cast<const SortingStep *>(&step))
        {
            info.kind = ConsumerStepInfo::Kind::Sorting;
            info.sorting_type = sorting->getType();
        }
        return info;
    }
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

FragmentDependency::Type classifyDependency(
    const ConsumerStepInfo & consumer, FragmentExchange::Kind kind, FragmentExchange::Mode mode, String & reason)
{
    reason.clear();

    if (kind == FragmentExchange::Kind::SetValues)
    {
        reason = "set must be complete";
        return FragmentDependency::Type::Blocking;
    }

    if (mode == FragmentExchange::Mode::Materialised)
    {
        reason = "materialised exchange";
        return FragmentDependency::Type::Blocking;
    }

    switch (consumer.kind)
    {
        case ConsumerStepInfo::Kind::Join:
            if (consumer.receive_feeds_build_side)
            {
                reason = "build side of " + consumer.step_name;
                return FragmentDependency::Type::Blocking;
            }
            return FragmentDependency::Type::Streaming;
        case ConsumerStepInfo::Kind::MergingAggregated:
            reason = "merge of aggregation states";
            return FragmentDependency::Type::Blocking;
        case ConsumerStepInfo::Kind::Sorting:
            if (consumer.sorting_type == SortingStep::Type::Full)
            {
                reason = "full sort";
                return FragmentDependency::Type::Blocking;
            }
            return FragmentDependency::Type::Streaming;
        case ConsumerStepInfo::Kind::Other:
            return FragmentDependency::Type::Streaming;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected kind of consumer step {}", consumer.step_name);
}

/// Scans with no dependencies come out at level 0 and the root at the top. The ids assigned by the
/// cutter already ascend from producers to consumers; the topological sort here does not rely on that
void computeLevels(FragmentGraph & graph)
{
    const size_t num_fragments = graph.fragments.size();

    std::unordered_map<UInt32, size_t> index_by_id;
    for (size_t i = 0; i < num_fragments; ++i)
    {
        if (!index_by_id.emplace(graph.fragments[i].id, i).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate fragment {} in the fragment graph", graph.fragments[i].id);
    }

    /// Kahn's algorithm over the edges producer -> consumer. Ready fragments are taken in the order of ids for determinism.
    std::vector<size_t> in_degree(num_fragments, 0);
    std::vector<std::vector<size_t>> consumers(num_fragments);
    for (size_t i = 0; i < num_fragments; ++i)
    {
        for (const auto & dependency : graph.fragments[i].depends_on)
        {
            auto it = index_by_id.find(dependency.producer);
            if (it == index_by_id.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} depends on unknown fragment {}",
                    graph.fragments[i].id, dependency.producer);
            consumers[it->second].push_back(i);
            ++in_degree[i];
        }
    }

    std::priority_queue<std::pair<UInt32, size_t>, std::vector<std::pair<UInt32, size_t>>, std::greater<>> ready;
    for (size_t i = 0; i < num_fragments; ++i)
        if (in_degree[i] == 0)
            ready.emplace(graph.fragments[i].id, i);

    std::vector<size_t> topological_order;
    topological_order.reserve(num_fragments);
    while (!ready.empty())
    {
        size_t i = ready.top().second;
        ready.pop();
        topological_order.push_back(i);
        for (size_t consumer : consumers[i])
            if (--in_degree[consumer] == 0)
                ready.emplace(graph.fragments[consumer].id, consumer);
    }

    if (topological_order.size() != num_fragments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has a cycle");

    /// The root is the fragment whose output nobody consumes: the result of the query.
    size_t num_roots = 0;
    size_t root = 0;
    for (size_t i = 0; i < num_fragments; ++i)
    {
        if (consumers[i].empty())
        {
            ++num_roots;
            root = i;
        }
    }
    if (num_roots != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has {} root fragments, expected one", num_roots);
    if (graph.fragments[root].role != QueryFragment::Role::Initiator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The root fragment F{} must have the initiator role", graph.fragments[root].id);

    /// A fragment is one level above its deepest producer.
    UInt32 max_level = 0;
    for (size_t i : topological_order)
    {
        auto & fragment = graph.fragments[i];
        fragment.level = 0;
        for (const auto & dependency : fragment.depends_on)
            fragment.level = std::max(fragment.level, graph.fragments[index_by_id.at(dependency.producer)].level + 1);
        max_level = std::max(max_level, fragment.level);
    }

    graph.levels.assign(max_level + 1, {});
    for (const auto & fragment : graph.fragments)
        graph.levels[fragment.level].push_back(fragment.id);
    for (auto & level : graph.levels)
        std::sort(level.begin(), level.end());
}

FragmentGraph buildFragmentGraph(const DistributedQueryPlan & distributed_plan)
{
    FragmentGraph graph;

    for (const auto & [stage_name, stage] : distributed_plan.stages)
    {
        QueryFragment fragment;
        fragment.id = stage.id;
        fragment.stage_name = stage_name;
        fragment.role = stage_name == main_stage_name ? QueryFragment::Role::Initiator : QueryFragment::Role::Worker;
        fragment.tasks = stage.tasks.size();
        fragment.plan = &stage.query_plan_fragment;
        fragment.summary = buildSummary(stage.query_plan_fragment);
        graph.fragments.push_back(std::move(fragment));
    }
    std::sort(graph.fragments.begin(), graph.fragments.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });
    for (size_t i = 1; i < graph.fragments.size(); ++i)
        if (graph.fragments[i - 1].id == graph.fragments[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stages {} and {} have the same id {}",
                graph.fragments[i - 1].stage_name, graph.fragments[i].stage_name, graph.fragments[i].id);

    auto find_fragment = [&](UInt32 id) -> QueryFragment &
    {
        auto it = std::lower_bound(graph.fragments.begin(), graph.fragments.end(), id,
            [](const QueryFragment & f, UInt32 value) { return f.id < value; });
        if (it == graph.fragments.end() || it->id != id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stage of fragment {} is referenced by a dependency, but does not exist", id);
        return *it;
    };

    for (const auto & [consumer_stage, producers] : distributed_plan.stage_depends_on)
    {
        UInt32 consumer_id = findStage(distributed_plan, consumer_stage).id;
        for (const auto & [producer_stage, exchange_name] : producers)
        {
            UInt32 producer_id = findStage(distributed_plan, producer_stage).id;
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
            exchange.estimated_rows = getEstimatedRows(*find_fragment(producer_id).plan);

            auto & consumer = find_fragment(consumer_id);
            ConsumerStepInfo consumer_step = findConsumerStep(*consumer.plan, consumer_stage, exchange_name);

            FragmentDependency dependency;
            dependency.producer = producer_id;
            dependency.exchange = exchange.id;
            dependency.type = classifyDependency(consumer_step, exchange.kind, exchange.mode, dependency.reason);
            consumer.depends_on.push_back(std::move(dependency));

            graph.exchanges.push_back(std::move(exchange));
        }
    }

    std::sort(graph.exchanges.begin(), graph.exchanges.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });
    for (size_t i = 1; i < graph.exchanges.size(); ++i)
        if (graph.exchanges[i - 1].id == graph.exchanges[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchanges {} and {} have the same id {}",
                graph.exchanges[i - 1].name, graph.exchanges[i].name, graph.exchanges[i].id);

    for (auto & fragment : graph.fragments)
        std::sort(fragment.depends_on.begin(), fragment.depends_on.end(),
            [](const auto & lhs, const auto & rhs) { return std::tie(lhs.producer, lhs.exchange) < std::tie(rhs.producer, rhs.exchange); });

    computeLevels(graph);
    return graph;
}

FragmentGraph buildSingleFragmentGraph(const QueryPlan & plan, std::optional<String> fallback_reason)
{
    FragmentGraph graph;

    QueryFragment fragment;
    fragment.id = 0;
    fragment.stage_name = String(main_stage_name);
    fragment.role = QueryFragment::Role::Initiator;
    fragment.location = QueryFragment::ExecutionLocation::SingleNode;
    fragment.fallback_reason = std::move(fallback_reason);
    fragment.tasks = 1;
    fragment.plan = &plan;
    fragment.summary = buildSummary(plan);
    graph.fragments.push_back(std::move(fragment));

    graph.levels = {{0}};
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
        fmt::format_to(out, "  F{} {:<{}}  {:<9}  {:<11}  tasks={}  level {}",
            fragment.id, fragment.stage_name, stage_name_width,
            fragment.role == QueryFragment::Role::Initiator ? "initiator" : "worker",
            fragment.location == QueryFragment::ExecutionLocation::Distributed ? "distributed" : "single node",
            fragment.tasks, fragment.level);

        for (size_t i = 0; i < fragment.depends_on.size(); ++i)
        {
            const auto & dependency = fragment.depends_on[i];
            fmt::format_to(out, "{}F{} via {} ({})", i == 0 ? "  needs " : ", ",
                dependency.producer, graph.getExchange(dependency.exchange).name,
                dependency.type == FragmentDependency::Type::Blocking ? "blocking: " + dependency.reason : "streaming");
        }
        if (fragment.fallback_reason)
            fmt::format_to(out, "  not distributed: {}", *fragment.fallback_reason);
        if (fragment.set)
            fmt::format_to(out, "  set: {}", *fragment.set);
        if (fragment.set_fragment)
            fmt::format_to(out, "  set source of F{}", *fragment.set_fragment);
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
        if (graph.exchanges[i].estimated_rows)
            fmt::format_to(out, "  ~{} rows", *graph.exchanges[i].estimated_rows);
        result += '\n';
    }

    result += "levels:";
    for (const auto & level : graph.levels)
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
