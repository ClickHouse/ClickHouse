#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/ScatterExchangeStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <base/types.h>
#include <optional>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CostConfig parseCostConfig(const String & json_str)
{
    CostConfig config;

    /// Rewrap any JSON parse / type error as a clear BAD_ARGUMENTS so an invalid override fails with a
    /// readable message instead of a leaked Poco exception.
    try
    {
        Poco::JSON::Parser parser;
        auto object = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();
        if (!object)
            throw Poco::Exception("value is not a JSON object");
        if (object->has("work_weight"))
            config.work_weight = object->getValue<Float64>("work_weight");
        else if (object->has("cpu_weight"))
            config.work_weight = object->getValue<Float64>("cpu_weight");
        if (object->has("network_weight"))
            config.network_weight = object->getValue<Float64>("network_weight");
        if (object->has("sequential_weight"))
            config.sequential_weight = object->getValue<Float64>("sequential_weight");
        if (object->has("exchange_fixed_overhead"))
            config.exchange_fixed_overhead = object->getValue<Float64>("exchange_fixed_overhead");
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid Cascades cost config '{}': {}", json_str, e.displayText());
    }

    /// Weights and the fixed overhead must be finite and non-negative. Zero is allowed (it lets a test
    /// ignore a dimension, e.g. `{"network_weight":0}`). A negative value is rejected: it would make
    /// more work look cheaper and can produce negative costs, which the optimizer's pruning relies on
    /// never happening.
    auto require = [&](Float64 value, const char * name)
    {
        if (!std::isfinite(value) || value < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cascades cost config: '{}' must be a finite non-negative number, got {}", name, value);
    };
    require(config.work_weight, "work_weight");
    require(config.network_weight, "network_weight");
    require(config.sequential_weight, "sequential_weight");
    require(config.exchange_fixed_overhead, "exchange_fixed_overhead");
    return config;
}

String CostConfig::dump() const
{
    Poco::JSON::Object obj;
    obj.set("work_weight", work_weight);
    obj.set("network_weight", network_weight);
    obj.set("sequential_weight", sequential_weight);
    obj.set("exchange_fixed_overhead", exchange_fixed_overhead);
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj.stringify(oss);
    return oss.str();
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static GroupPtr getInputGroupWithStats(Memo & memo, const GroupExpressionPtr & expression, size_t input_index)
{
    auto input_group = memo.getGroup(expression->inputs[input_index].group_id);
    if (!input_group->statistics.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for input group #{} of expression '{}' (group #{}).\n"
            "Input group state:\n{}",
            expression->inputs[input_index].group_id, expression->getDescription(), expression->group_id, input_group->dump(memo.getCostConfig()));
    return input_group;
}

struct PartialTopNInputInfo
{
    Float64 limit;       /// per-node top-N bound L
    Float64 input_rows;  /// total rows feeding the partial sort, before trimming to L
};

/// If the given input of `expression` is a partial top-N group (built by TwoStageTopN), return the
/// per-node limit L and the total input row count. A sorted gather over it transfers up to
/// min(input_rows, L * node_count) rows: each of the N producer nodes emits at most L, but no more
/// than the input holds.
static std::optional<PartialTopNInputInfo> getPartialTopNInputInfo(Memo & memo, const GroupExpressionPtr & expression, size_t input_index)
{
    auto input_group = memo.getGroup(expression->inputs[input_index].group_id);
    for (const auto & candidate : input_group->physical_expressions)
    {
        if (!dynamic_cast<const PartialTopNStrategy *>(candidate->strategy.get()))
            continue;
        const auto * sort = typeid_cast<const SortingStep *>(candidate->getQueryPlanStep());
        if (!sort || candidate->inputs.empty())
            return std::nullopt;
        auto sort_input = memo.getGroup(candidate->inputs[0].group_id);
        if (!sort_input->statistics.has_value())
            return std::nullopt;
        return PartialTopNInputInfo{Float64(sort->getLimit()), sort_input->statistics->estimated_row_count};
    }
    return std::nullopt;
}

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    auto group = memo.getGroup(expression->group_id);

    /// Statistics should have been derived before calling estimateCost
    if (!group->statistics.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for group #{} (expression '{}') before estimateCost.\n"
            "Group state:\n{}",
            expression->group_id, expression->getDescription(), group->dump(memo.getCostConfig()));

    const Float64 distribution_node_count = static_cast<Float64>(std::max<size_t>(expression->properties.distribution.node_count, 1));
    /// Partitioned = 1/N per node; replicated = full work per node.
    const Float64 parallelism = expression->properties.distribution.is_replicated
        ? 1.0
        : distribution_node_count;

    ExpressionCost total_cost;
    const IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(expression_plan_step))
    {
        const auto & left_input = expression->inputs[0];
        const auto & right_input = expression->inputs[1];
        auto left_input_group = memo.getGroup(left_input.group_id);
        auto right_input_group = memo.getGroup(right_input.group_id);
        const auto * join_strategy = dynamic_cast<const IJoinStrategy *>(expression->strategy.get());
        total_cost = estimateHashJoinCost(*join_step, join_strategy, *group->statistics, *left_input_group->statistics, *right_input_group->statistics, parallelism);
    }
    else if (const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression_plan_step))
    {
        const auto * read_strategy = dynamic_cast<const IReadStrategy *>(expression->strategy.get());
        total_cost = estimateReadCost(*read_step, read_strategy, *group->statistics, distribution_node_count);
    }
    else if (typeid_cast<const FilterStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        total_cost.cost.work = 0.1 * input_group->statistics->estimated_row_count / parallelism;
    }
    else if (typeid_cast<const ExpressionStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        total_cost.cost.work = 0.1 * input_group->statistics->estimated_row_count / parallelism;
    }
    else if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        const auto * aggregation_strategy = dynamic_cast<const IAggregationStrategy *>(expression->strategy.get());
        total_cost = estimateAggregationCost(*aggregating_step, aggregation_strategy, *group->statistics, *input_group->statistics, parallelism);
    }
    else if (typeid_cast<const MergingAggregatedStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        total_cost.cost.work = (group->statistics->estimated_row_count
            + group->statistics->estimated_row_count * group->statistics->estimated_bytes_per_row
            + input_group->statistics->estimated_row_count) / parallelism;
        /// Sequential ~ output groups (hash table size). Penalizes gather-to-one-node
        /// merge for large outputs; bucket-level merge within a node is parallel.
        total_cost.cost.sequential = group->statistics->estimated_row_count / parallelism;
    }
    else if (dynamic_cast<const BroadcastExchangeStep *>(expression_plan_step))
    {
        auto bytes_per_row = group->statistics->estimated_bytes_per_row;
        /// Each of the N receiving nodes gets a full copy, so N times the data crosses the network;
        /// without the factor a 100-node broadcast would look as cheap as a 2-node one.
        const Float64 receiver_count = Float64(std::max<size_t>(1, expression->properties.distribution.node_count));
        total_cost.cost.network += group->statistics->estimated_row_count * bytes_per_row * receiver_count;
        total_cost.cost.sequential += memo.getCostConfig().exchange_fixed_overhead;
    }
    else if (dynamic_cast<const LogicalExchangeStep *>(expression_plan_step))
    {
        Float64 rows = group->statistics->estimated_row_count;
        /// A sorted gather over a partial top-N receives up to L rows from EACH producer node, so it
        /// transfers min(input_rows, L * node_count) rows, not the group's trimmed L.
        if (dynamic_cast<const GatherExchangeStep *>(expression_plan_step))
        {
            if (auto partial = getPartialTopNInputInfo(memo, expression, 0))
            {
                const Float64 producer_node_count = std::max<Float64>(1, Float64(expression->inputs[0].required_properties.distribution.node_count));
                rows = std::min(partial->input_rows, partial->limit * producer_node_count);
            }
        }
        const auto bytes_per_row = group->statistics->estimated_bytes_per_row;
        /// Each row crosses the network once.
        total_cost.cost.network += rows * bytes_per_row;
        total_cost.cost.sequential += memo.getCostConfig().exchange_fixed_overhead;
        /// Gather (N->1) and Scatter (1->N) funnel every row through a single node that
        /// sends or receives them sequentially; Shuffle (N->N) spreads this across nodes.
        /// Without the funnel cost a gather/scatter of a large input looks as cheap as a
        /// shuffle, so the optimizer distributes work (e.g. a sort) that should stay local.
        if (dynamic_cast<const GatherExchangeStep *>(expression_plan_step)
            || dynamic_cast<const ScatterExchangeStep *>(expression_plan_step))
            total_cost.cost.sequential += rows;
    }
    else if (typeid_cast<const SortingStep *>(expression_plan_step))
    {
        Float64 rows = group->statistics->estimated_row_count;
        /// A partial top-N sort scans all of its input rows (keeping only the top L per node), so cost
        /// its work on the input cardinality rather than the trimmed output L.
        if (dynamic_cast<const PartialTopNStrategy *>(expression->strategy.get()))
            rows = getInputGroupWithStats(memo, expression, 0)->statistics->estimated_row_count;
        total_cost.cost.work += rows * std::max(1.0, std::log2(rows)) / parallelism;
        /// N-way merge is single-threaded.
        total_cost.cost.sequential += rows / parallelism;
    }
    else
    {
        if (expression->inputs.empty())
        {
            /// Some default non-zero cost
            total_cost.cost.work = 100500;
        }
    }

    total_cost.subtree_cost = total_cost.cost;

    /// Add input subtree costs. Unsatisfiable inputs produce infinity.
    for (const auto & input : expression->inputs)
    {
        ExpressionWithCost best;
        if (input.group_id == expression->group_id)
            /// Self-referential enforcer input: price it against an acyclic source (excluding
            /// itself), so the cost reflects a plan that can actually be built.
            best = memo.getGroup(input.group_id)->selectInputImplementation(
                input.required_properties, memo.getCostConfig(),
                std::unordered_set<GroupExpression *>{expression.get()}, /*input_is_self_referential=*/true);
        else
            best = memo.getGroup(input.group_id)->getBestImplementation(input.required_properties, memo.getCostConfig());

        if (!best.expression)
        {
            total_cost.subtree_cost = Cost::infinity();
            return total_cost;
        }
        total_cost.subtree_cost += best.cost.subtree_cost;
    }

    return total_cost;
}

ExpressionCost CostEstimator::estimateHashJoinCost(
    const JoinStepLogical & /*join_step*/,
        const IJoinStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics,
        Float64 parallelism)
{
    const bool is_broadcast = dynamic_cast<const BroadcastJoinStrategy *>(strategy) != nullptr;

    ExpressionCost join_cost;

    /// Hash join: left probe + right build (2x) + output.
    join_cost.cost.work = (left_statistics.estimated_row_count
                           + 2.0 * right_statistics.estimated_row_count
                           + this_step_statistics.estimated_row_count) / parallelism;

    /// Hash table materialization: memory allocation + cache pressure.
    const Float64 ht_bytes = right_statistics.estimated_row_count * right_statistics.estimated_bytes_per_row;

    if (is_broadcast)
    {
        /// Full right table per node. Network modeled by BroadcastExchange.
        join_cost.cost.work += ht_bytes;
        join_cost.cost.sequential += right_statistics.estimated_row_count * 2.0;
    }
    else
    {
        /// 1/N of right table per node. Network modeled by ShuffleExchange.
        join_cost.cost.work += ht_bytes / parallelism;
        join_cost.cost.sequential += right_statistics.estimated_row_count * 2.0 / parallelism;
    }

    return join_cost;
}

ExpressionCost CostEstimator::estimateReadCost(
    const ReadFromMergeTree & /*read_step*/,
    const IReadStrategy * strategy,
    const ExpressionStatistics & this_step_statistics,
    Float64 distribution_node_count)
{
    auto bytes_per_row = this_step_statistics.estimated_bytes_per_row;

    if (dynamic_cast<const ParallelReadStrategy *>(strategy) != nullptr)
    {
        /// Each of N nodes reads 1/N.
        return ExpressionCost{
            .cost = Cost{.work = this_step_statistics.estimated_row_count * bytes_per_row / distribution_node_count},
            .subtree_cost = {},
        };
    }

    if (dynamic_cast<const ReplicatedReadStrategy *>(strategy) != nullptr)
    {
        /// Shared storage: every node reads full table from S3. No network.
        return ExpressionCost{
            .cost = Cost{.work = this_step_statistics.estimated_row_count * bytes_per_row},
            .subtree_cost = {},
        };
    }

    /// Single-node local read.
    return ExpressionCost{
        .cost = Cost{.work = this_step_statistics.estimated_row_count * bytes_per_row},
        .subtree_cost = {},
    };
}

ExpressionCost CostEstimator::estimateAggregationCost(
    const AggregatingStep & /*aggregating_step*/,
    const IAggregationStrategy * strategy,
    const ExpressionStatistics & this_step_statistics,
    const ExpressionStatistics & input_statistics,
    Float64 parallelism)
{
    const bool is_local = dynamic_cast<const LocalAggregationStrategy *>(strategy) != nullptr;
    const bool is_shuffle = dynamic_cast<const ShuffleAggregationStrategy *>(strategy) != nullptr;
    const bool is_partial = dynamic_cast<const PartialAggregationStrategy *>(strategy) != nullptr;

    ExpressionCost aggregation_cost;

    if (is_local)
    {
        /// Hash table build + probe + output materialization.
        aggregation_cost.cost.work +=
            this_step_statistics.estimated_row_count +
            this_step_statistics.estimated_row_count * this_step_statistics.estimated_bytes_per_row +
            input_statistics.estimated_row_count;
    }
    else if (is_shuffle)
    {
        /// Per-node 1/N. Network modeled by ShuffleExchange child.
        aggregation_cost.cost.work +=
            this_step_statistics.estimated_row_count / parallelism +
            this_step_statistics.estimated_row_count * this_step_statistics.estimated_bytes_per_row / parallelism +
            input_statistics.estimated_row_count / parallelism;
    }
    else if (is_partial)
    {
        aggregation_cost.cost.work +=
            this_step_statistics.estimated_row_count / parallelism +
            this_step_statistics.estimated_row_count * this_step_statistics.estimated_bytes_per_row / parallelism +
            input_statistics.estimated_row_count / parallelism;
    }
    else
    {
        /// Fallback (e.g. DefaultImplementation). Same as Local.
        aggregation_cost.cost.work +=
            this_step_statistics.estimated_row_count +
            this_step_statistics.estimated_row_count * this_step_statistics.estimated_bytes_per_row +
            input_statistics.estimated_row_count;
    }

    return aggregation_cost;
}

}
