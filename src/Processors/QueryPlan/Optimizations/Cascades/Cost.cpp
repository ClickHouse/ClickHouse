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
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <base/types.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>

namespace DB
{

CostConfig parseCostConfig(const String & json_str)
{
    CostConfig config;
    Poco::JSON::Parser parser;
    auto result = parser.parse(json_str);
    const auto & object = result.extract<Poco::JSON::Object::Ptr>();
    if (!object)
        return config;
    if (object->has("cpu_weight"))
        config.cpu_weight = object->getValue<Float64>("cpu_weight");
    if (object->has("memory_weight"))
        config.memory_weight = object->getValue<Float64>("memory_weight");
    if (object->has("network_weight"))
        config.network_weight = object->getValue<Float64>("network_weight");
    if (object->has("io_weight"))
        config.io_weight = object->getValue<Float64>("io_weight");
    if (object->has("sequential_weight"))
        config.sequential_weight = object->getValue<Float64>("sequential_weight");
    if (object->has("exchange_fixed_overhead"))
        config.exchange_fixed_overhead = object->getValue<Float64>("exchange_fixed_overhead");
    return config;
}

String CostConfig::dump() const
{
    Poco::JSON::Object obj;
    obj.set("cpu_weight", cpu_weight);
    obj.set("memory_weight", memory_weight);
    obj.set("network_weight", network_weight);
    obj.set("io_weight", io_weight);
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
    /// Effective parallelism: partitioned data is split across N nodes (each does 1/N),
    /// replicated data is duplicated on each node (each does the full work).
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
        total_cost.cost.cpu = 0.1 * input_group->statistics->estimated_row_count / parallelism;
    }
    else if (typeid_cast<const ExpressionStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        total_cost.cost.cpu = 0.1 * input_group->statistics->estimated_row_count / parallelism;
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
        /// Merging intermediate aggregate states: CPU proportional to input + output rows.
        /// At N nodes each node merges 1/N of the data (replicated = no parallelism benefit).
        total_cost.cost.cpu = (group->statistics->estimated_row_count + input_group->statistics->estimated_row_count) / parallelism;
        /// Merge phase is sequential (single-threaded merge of partial states).
        total_cost.cost.sequential += input_group->statistics->estimated_row_count / parallelism;
    }
    else if (dynamic_cast<const BroadcastExchangeStep *>(expression_plan_step))
    {
        auto bytes_per_row = group->statistics->estimated_bytes_per_row;
        /// Broadcast replicates all rows to every destination node.
        /// Per-node receive bottleneck: each node receives and materializes one full copy.
        total_cost.cost.network += group->statistics->estimated_row_count * bytes_per_row;
        /// Per-node memory: each destination materializes one copy.
        total_cost.cost.memory += group->statistics->estimated_row_count * bytes_per_row;
        /// Fixed overhead for connection setup / metadata exchange is sequential.
        total_cost.cost.sequential += memo.getCostConfig().exchange_fixed_overhead;
    }
    else if (dynamic_cast<const LogicalExchangeStep *>(expression_plan_step))
    {
        auto bytes_per_row = group->statistics->estimated_bytes_per_row;
        /// Gather, Shuffle, Scatter: each row is sent exactly once; cost is proportional to data volume.
        total_cost.cost.network += group->statistics->estimated_row_count * bytes_per_row;
        /// Fixed overhead for connection setup / metadata exchange is sequential.
        total_cost.cost.sequential += memo.getCostConfig().exchange_fixed_overhead;
    }
    else if (typeid_cast<const SortingStep *>(expression_plan_step))
    {
        /// Sorting: N log N CPU cost (parallel partial sort across streams).
        /// At N nodes each node sorts 1/N of the data (replicated = no parallelism benefit).
        Float64 rows = group->statistics->estimated_row_count;
        total_cost.cost.cpu += rows * std::max(1.0, std::log2(rows)) / parallelism;
        /// Final N-way merge (`MergingSortedTransform`) is single-threaded.
        total_cost.cost.sequential += rows / parallelism;
    }
    else
    {
        if (expression->inputs.empty())
        {
            /// Some default non-zero cost
            total_cost.cost.cpu = 100500;
        }
    }

    /// Subtree cost starts with the own cost of this expression, then children are added
    total_cost.subtree_cost = total_cost.cost;

    /// Add costs of all inputs.  If any input has no satisfying implementation
    /// (e.g. a leaf group that can't go multi-node), return infinity cost so
    /// this expression is never selected as the best.
    for (const auto & input : expression->inputs)
    {
        auto best = memo.getGroup(input.group_id)->getBestImplementation(input.required_properties, memo.getCostConfig());
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
    const bool is_merge_join = dynamic_cast<const LocalMergeJoinStrategy *>(strategy) != nullptr
        || dynamic_cast<const ShuffleMergeJoinStrategy *>(strategy) != nullptr;

    ExpressionCost join_cost;

    if (is_merge_join)
    {
        /// Linear scan of both sorted inputs + output materialization. No hash table.
        join_cost.cost.cpu = (left_statistics.estimated_row_count
                              + right_statistics.estimated_row_count
                              + this_step_statistics.estimated_row_count) / parallelism;
        /// The merge is single-threaded (two cursors walking sorted streams).
        join_cost.cost.sequential = (left_statistics.estimated_row_count
                                     + right_statistics.estimated_row_count) / parallelism;
        return join_cost;
    }

    /// Hash join: probe scan + build phase (hash + insert) + output materialization.
    join_cost.cost.cpu = (left_statistics.estimated_row_count
                          + 2.0 * right_statistics.estimated_row_count
                          + this_step_statistics.estimated_row_count) / parallelism;

    if (is_broadcast)
    {
        /// Broadcast: each node holds the FULL right table (replicated to all nodes).
        /// Network cost is already modeled by the BroadcastExchange expression.
        join_cost.cost.memory += right_statistics.estimated_row_count * right_statistics.estimated_bytes_per_row;
        /// Build phase is sequential: each node inserts the full right table into the hash table.
        join_cost.cost.sequential += right_statistics.estimated_row_count * 2.0;
    }
    else
    {
        /// Shuffle or local: each node holds 1/N of the right table (or full if replicated).
        /// Network cost (for shuffle) is already modeled by ShuffleExchange expressions.
        join_cost.cost.memory += right_statistics.estimated_row_count * right_statistics.estimated_bytes_per_row / parallelism;
        /// Build phase is sequential: each node builds hash table from its share.
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
        /// Parallel read: each of N nodes reads 1/N of the data.
        return ExpressionCost{
            .cost = Cost{.io = this_step_statistics.estimated_row_count * bytes_per_row / distribution_node_count},
            .subtree_cost = {},
        };
    }

    if (dynamic_cast<const SortedReadStrategy *>(strategy) != nullptr)
    {
        /// Same IO as regular read. Small CPU overhead for merge-sorting part streams
        /// (N-way merge of already-sorted parts). No sequential cost — the merge is
        /// lightweight compared to the IO savings from eliminating an explicit Sort.
        Float64 rows = this_step_statistics.estimated_row_count;
        Float64 io = rows * bytes_per_row / distribution_node_count;
        return ExpressionCost{
            .cost = Cost{.cpu = rows * 0.1 / distribution_node_count, .io = io},
            .subtree_cost = {},
        };
    }

    if (dynamic_cast<const ReplicatedReadStrategy *>(strategy) != nullptr)
    {
        /// Replicated read on shared storage: every node reads the full table from
        /// object storage.  IO cost is NOT divided by N — each node does a full scan.
        /// No network cost — data is accessed directly from S3, not transferred between nodes.
        return ExpressionCost{
            .cost = Cost{.io = this_step_statistics.estimated_row_count * bytes_per_row},
            .subtree_cost = {},
        };
    }

    /// Default: single-node local read.
    return ExpressionCost{
        .cost = Cost{.io = this_step_statistics.estimated_row_count * bytes_per_row},
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
    const bool is_streaming = dynamic_cast<const StreamingAggregationStrategy *>(strategy) != nullptr;

    ExpressionCost aggregation_cost;

    if (is_streaming)
    {
        /// Linear scan of sorted input, no hash table, no memory.
        aggregation_cost.cost.cpu += input_statistics.estimated_row_count / parallelism;
    }
    else if (is_local)
    {
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count +
            input_statistics.estimated_row_count;
    }
    else if (is_shuffle)
    {
        /// CPU: per-node computation on 1/N of the data.
        /// Network cost is NOT added here -- it is already modeled by the `ShuffleExchange` child.
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count / parallelism +
            input_statistics.estimated_row_count / parallelism;
    }
    else if (is_partial)
    {
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count / parallelism +
            input_statistics.estimated_row_count / parallelism;
    }
    else
    {
        /// Fallback: no recognized strategy (e.g. DefaultImplementation passthrough).
        /// Same cost model as Local.
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count +
            input_statistics.estimated_row_count;
    }

    return aggregation_cost;
}

}
