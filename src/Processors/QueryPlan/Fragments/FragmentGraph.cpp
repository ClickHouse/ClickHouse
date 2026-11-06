#include <Processors/QueryPlan/Fragments/FragmentGraph.h>

#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <cmath>

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

/// The index of the fragment with the given id in `fragments`, which is sorted by id; nullopt when absent.
std::optional<size_t> findFragmentIndex(const FragmentGraph & graph, UInt32 id)
{
    auto it = std::lower_bound(graph.fragments.begin(), graph.fragments.end(), id,
        [](const QueryFragment & f, UInt32 value) { return f.id < value; });
    if (it == graph.fragments.end() || it->id != id)
        return std::nullopt;
    return it - graph.fragments.begin();
}

/// Validates the graph (see `validateFragmentGraph`) and sorts it producers first, as `computeLevels` needs. The sort
/// detects cycles. The ids assigned by the cutter mostly ascend from producers to consumers, but the order of ids is
/// not topological, because the fragments of `IN`-subquery sets get ids after the fragments that read them.
TopologicalOrder sortTopologically(const FragmentGraph & graph)
{
    const size_t num_fragments = graph.fragments.size();

    /// Strictly ascending ids: unique, and sorted as the binary searches of `getFragment` and `getExchange` need.
    for (size_t i = 1; i < num_fragments; ++i)
        if (graph.fragments[i - 1].id >= graph.fragments[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragments {} (id {}) and {} (id {}) are not in strictly ascending order of ids",
                graph.fragments[i - 1].stage_name, graph.fragments[i - 1].id, graph.fragments[i].stage_name, graph.fragments[i].id);
    for (size_t i = 1; i < graph.exchanges.size(); ++i)
        if (graph.exchanges[i - 1].id >= graph.exchanges[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchanges {} (id {}) and {} (id {}) are not in strictly ascending order of ids",
                graph.exchanges[i - 1].name, graph.exchanges[i - 1].id, graph.exchanges[i].name, graph.exchanges[i].id);

    TopologicalOrder result;
    result.producers.resize(num_fragments);
    std::vector<size_t> in_degree(num_fragments, 0);
    std::vector<std::vector<size_t>> consumers(num_fragments);
    for (const auto & exchange : graph.exchanges)
    {
        auto producer = findFragmentIndex(graph, exchange.from);
        if (!producer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} comes from unknown fragment {}", exchange.name, exchange.from);
        auto consumer = findFragmentIndex(graph, exchange.to);
        if (!consumer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} goes to unknown fragment {}", exchange.name, exchange.to);
        consumers[*producer].push_back(*consumer);
        result.producers[*consumer].push_back(*producer);
        ++in_degree[*consumer];
    }

    /// Kahn's algorithm, with `order` as the queue of fragments whose producers are all taken. The fragments of a
    /// cycle never become ready, so `order` ends up short.
    result.order.reserve(num_fragments);
    for (size_t i = 0; i < num_fragments; ++i)
        if (in_degree[i] == 0)
            result.order.push_back(i);

    for (size_t pos = 0; pos < result.order.size(); ++pos)
        for (size_t consumer : consumers[result.order[pos]])
            if (--in_degree[consumer] == 0)
                result.order.push_back(consumer);

    if (result.order.size() != num_fragments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has a cycle");

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

    /// The fragments are sorted by id, so every level comes out ascending.
    std::vector<std::vector<UInt32>> levels(max_level + 1);
    for (size_t i = 0; i < graph.fragments.size(); ++i)
        levels[level_of[i]].push_back(graph.fragments[i].id);
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

        checkNotEmpty(stage.query_plan_fragment, stage_name);
        graph.fragments.push_back(QueryFragment{
            .id = stage.id,
            .stage_name = stage_name,
            .execution_mode = QueryFragment::ExecutionMode::Distributed,
            .fallback_reason = {},
            .description = {},
            .tasks = stage.tasks.size(),
            .plan = &stage.query_plan_fragment,
        });
    }
    /// `stages` and `stage_depends_on` are unordered maps: sort for a deterministic order and for the lookups by id.
    /// Duplicate ids are left to `validateFragmentGraph`.
    std::sort(graph.fragments.begin(), graph.fragments.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });

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

            const auto kind = getExchangeKind(description);

            graph.exchanges.push_back(FragmentExchange{
                .id = description.id,
                .name = description.name,
                .from = producer_id,
                .to = consumer_id,
                .kind = kind,
                .keys = kind == FragmentExchange::Kind::Hash ? description.keys : Names{},
                .merge_sort = kind == FragmentExchange::Kind::Gather ? description.sort_description : std::nullopt,
                .mode = description.kind,
                .source_buckets = description.source_bucket_count,
                .destination_buckets = description.destination_bucket_count,
                .statistics = {.estimated_rows = getEstimatedRows(*graph.getFragment(producer_id).plan)},
            });
        }
    }

    std::sort(graph.exchanges.begin(), graph.exchanges.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });

    validateFragmentGraph(graph);
    return graph;
}

FragmentGraph buildSingleFragmentGraph(const QueryPlan & plan, std::optional<String> fallback_reason)
{
    FragmentGraph graph;

    checkNotEmpty(plan, String(main_stage_name));
    graph.fragments.push_back(QueryFragment{
        .id = 0,
        .stage_name = String(main_stage_name),
        .execution_mode = QueryFragment::ExecutionMode::SingleNode,
        .fallback_reason = std::move(fallback_reason),
        .description = {},
        .tasks = 1,
        .plan = &plan,
    });
    return graph;
}

}
