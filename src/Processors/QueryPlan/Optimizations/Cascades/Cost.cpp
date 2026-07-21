#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/logger_useful.h>
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

        auto read = [&](const char * name, Float64 & value)
        {
            if (object->has(name))
                value = object->getValue<Float64>(name);
        };
        read("work_weight", config.work_weight);
        if (!object->has("work_weight"))
            read("cpu_weight", config.work_weight);
        read("network_weight", config.network_weight);
        read("sequential_weight", config.sequential_weight);
        read("exchange_fixed_overhead", config.exchange_fixed_overhead);
        read("expression_cost_per_row", config.expression_cost_per_row);
        read("hash_table_build_factor", config.hash_table_build_factor);
        read("unknown_leaf_cost", config.unknown_leaf_cost);
        read("funnel_sequential_cost_per_row", config.funnel_sequential_cost_per_row);
        read("merge_sequential_cost_per_row", config.merge_sequential_cost_per_row);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid Cascades cost config '{}': {}", json_str, e.displayText());
    }

    /// All values must be finite and non-negative. Zero is allowed (it lets a test ignore a
    /// dimension, e.g. `{"network_weight":0}`). A negative value is rejected: it would make
    /// more work look cheaper and can produce negative costs, which the optimizer's pruning
    /// relies on never happening.
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
    require(config.expression_cost_per_row, "expression_cost_per_row");
    require(config.hash_table_build_factor, "hash_table_build_factor");
    require(config.unknown_leaf_cost, "unknown_leaf_cost");
    require(config.funnel_sequential_cost_per_row, "funnel_sequential_cost_per_row");
    require(config.merge_sequential_cost_per_row, "merge_sequential_cost_per_row");
    return config;
}

String CostConfig::dump() const
{
    Poco::JSON::Object obj;
    obj.set("work_weight", work_weight);
    obj.set("network_weight", network_weight);
    obj.set("sequential_weight", sequential_weight);
    obj.set("exchange_fixed_overhead", exchange_fixed_overhead);
    obj.set("expression_cost_per_row", expression_cost_per_row);
    obj.set("hash_table_build_factor", hash_table_build_factor);
    obj.set("unknown_leaf_cost", unknown_leaf_cost);
    obj.set("funnel_sequential_cost_per_row", funnel_sequential_cost_per_row);
    obj.set("merge_sequential_cost_per_row", merge_sequential_cost_per_row);
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj.stringify(oss);
    return oss.str();
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Reads the statistics of the given input, throwing if the input group had none derived.
static const ExpressionStatistics & inputStats(const CostInputs & inputs, size_t input_index)
{
    if (input_index >= inputs.input_stats.size() || !inputs.input_stats[input_index])
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for input #{} of step '{}'",
            input_index, inputs.step ? inputs.step->getName() : "<none>");
    return *inputs.input_stats[input_index];
}

/// Hash join: left probe + right build + output. The build side is materialized fully per node
/// for a broadcast join and 1/N per node otherwise. Network is modeled by the exchange steps.
static Cost hashJoinCost(const CostInputs & inputs, bool is_broadcast)
{
    const auto & left_stats = inputStats(inputs, 0);
    const auto & right_stats = inputStats(inputs, 1);

    Cost cost;
    cost.work = (left_stats.estimated_row_count
                 + inputs.config.hash_table_build_factor * right_stats.estimated_row_count
                 + inputs.output_stats.estimated_row_count) / inputs.parallelism;

    /// Hash table materialization: memory allocation + cache pressure.
    const Float64 hash_table_bytes = right_stats.estimated_row_count * right_stats.estimated_bytes_per_row;
    if (is_broadcast)
    {
        cost.work += hash_table_bytes;
        cost.sequential += inputs.config.hash_table_build_factor * right_stats.estimated_row_count;
    }
    else
    {
        cost.work += hash_table_bytes / inputs.parallelism;
        cost.sequential += inputs.config.hash_table_build_factor * right_stats.estimated_row_count / inputs.parallelism;
    }
    return cost;
}

/// One node reads the whole table.
static Cost fullReadCost(const CostInputs & inputs)
{
    return Cost{.work = inputs.output_stats.estimated_row_count * inputs.output_stats.estimated_bytes_per_row};
}

/// Hash table build + probe + output materialization, divided when each node handles 1/N.
static Cost aggregationCost(const CostInputs & inputs, Float64 divide_by)
{
    Cost cost;
    cost.work = (inputs.output_stats.estimated_row_count
                 + inputs.output_stats.estimated_row_count * inputs.output_stats.estimated_bytes_per_row
                 + inputStats(inputs, 0).estimated_row_count) / divide_by;
    return cost;
}

Cost LocalJoinStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return hashJoinCost(inputs, /*is_broadcast=*/false);
}

Cost BroadcastJoinStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return hashJoinCost(inputs, /*is_broadcast=*/true);
}

Cost ShuffleJoinStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return hashJoinCost(inputs, /*is_broadcast=*/false);
}

Cost LocalAggregationStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return aggregationCost(inputs, 1.0);
}

Cost ShuffleAggregationStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return aggregationCost(inputs, inputs.parallelism);
}

Cost PartialAggregationStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return aggregationCost(inputs, inputs.parallelism);
}

Cost ParallelReadStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    /// Each of N nodes reads 1/N.
    Cost cost = fullReadCost(inputs);
    cost.work /= inputs.node_count;
    return cost;
}

Cost ReplicatedReadStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    /// Shared storage: every node reads the full table from S3. No network.
    return fullReadCost(inputs);
}

static Cost mergingAggregatedCost(const CostInputs & inputs)
{
    Cost cost;
    cost.work = (inputs.output_stats.estimated_row_count
        + inputs.output_stats.estimated_row_count * inputs.output_stats.estimated_bytes_per_row
        + inputStats(inputs, 0).estimated_row_count) / inputs.parallelism;
    /// Sequential ~ output groups (hash table size). Penalizes gather-to-one-node
    /// merge for large outputs; bucket-level merge within a node is parallel.
    cost.sequential = inputs.output_stats.estimated_row_count / inputs.parallelism;
    return cost;
}

static Cost broadcastExchangeCost(const CostInputs & inputs)
{
    Cost cost;
    /// Each of the N receiving nodes gets a full copy, so N times the data crosses the network;
    /// without the factor a 100-node broadcast would look as cheap as a 2-node one.
    cost.network += inputs.output_stats.estimated_row_count * inputs.output_stats.estimated_bytes_per_row
        * std::max(1.0, inputs.node_count);
    cost.sequential += inputs.config.exchange_fixed_overhead;
    return cost;
}

static Cost exchangeCost(const CostInputs & inputs, const IQueryPlanStep & step)
{
    /// A sorted gather over a partial top-N transfers min(input_rows, L * node_count) rows,
    /// not the group's trimmed L; the caller resolves that override from the memo.
    const Float64 rows = inputs.exchange_rows_override.value_or(inputs.output_stats.estimated_row_count);
    Cost cost;
    /// Each row crosses the network once.
    cost.network += rows * inputs.output_stats.estimated_bytes_per_row;
    cost.sequential += inputs.config.exchange_fixed_overhead;
    /// Gather (N->1) and Scatter (1->N) funnel every row through a single node that
    /// sends or receives them sequentially; Shuffle (N->N) spreads this across nodes.
    /// Without the funnel cost a gather/scatter of a large input looks as cheap as a
    /// shuffle, so the optimizer distributes work (e.g. a sort) that should stay local.
    if (dynamic_cast<const GatherExchangeStep *>(&step) || dynamic_cast<const ScatterExchangeStep *>(&step))
        cost.sequential += inputs.config.funnel_sequential_cost_per_row * rows;
    return cost;
}

static Cost sortCost(const CostInputs & inputs, const SortingStep & sorting_step)
{
    Float64 rows = inputs.output_stats.estimated_row_count;
    /// A bounded (top-N) sort scans all of its input rows, keeping only the best L; the group
    /// stats are already trimmed to L, so read the input cardinality from the input.
    if (sorting_step.getLimit() > 0)
        rows = inputStats(inputs, 0).estimated_row_count;
    /// A top-N keeps a heap of at most L rows (n * log L); a full sort is n * log n.
    const Float64 sorted_rows = sorting_step.getLimit() > 0
        ? std::min(rows, Float64(sorting_step.getLimit()))
        : rows;
    Cost cost;
    cost.work += rows * std::max(1.0, std::log2(sorted_rows)) / inputs.parallelism;
    /// N-way merge is single-threaded and sees only the rows the sort emits.
    cost.sequential += inputs.config.merge_sequential_cost_per_row * inputs.output_stats.estimated_row_count / inputs.parallelism;
    return cost;
}

/// Steps with a physical strategy price themselves; the rest are matched here - exchanges by
/// `dynamic_cast` (they form a hierarchy under `LogicalExchangeStep`), other steps by
/// `typeid_cast` (exact types).
Cost estimateOperatorCost(const CostInputs & inputs, const IImplementationStrategy * strategy)
{
    const IQueryPlanStep * step = inputs.step;

    if (typeid_cast<const JoinStepLogical *>(step))
    {
        if (const auto * join_strategy = dynamic_cast<const IJoinStrategy *>(strategy))
            return join_strategy->estimateOperatorCost(inputs);
        /// A logical join not yet given a physical strategy: price as a non-broadcast hash join.
        return hashJoinCost(inputs, /*is_broadcast=*/false);
    }

    if (typeid_cast<const ReadFromMergeTree *>(step))
    {
        if (const auto * read_strategy = dynamic_cast<const IReadStrategy *>(strategy))
            return read_strategy->estimateOperatorCost(inputs);
        /// Single-node local read.
        return fullReadCost(inputs);
    }

    if (typeid_cast<const FilterStep *>(step) || typeid_cast<const ExpressionStep *>(step))
        return Cost{.work = inputs.config.expression_cost_per_row * inputStats(inputs, 0).estimated_row_count / inputs.parallelism};

    if (typeid_cast<const AggregatingStep *>(step))
    {
        if (const auto * aggregation_strategy = dynamic_cast<const IAggregationStrategy *>(strategy))
            return aggregation_strategy->estimateOperatorCost(inputs);
        /// An aggregation without a strategy (e.g. from `DefaultImplementation`): price as Local.
        return aggregationCost(inputs, 1.0);
    }

    if (typeid_cast<const MergingAggregatedStep *>(step))
        return mergingAggregatedCost(inputs);

    if (dynamic_cast<const BroadcastExchangeStep *>(step))
        return broadcastExchangeCost(inputs);

    if (dynamic_cast<const LogicalExchangeStep *>(step))
        return exchangeCost(inputs, *step);

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
        return sortCost(inputs, *sorting_step);

    if (inputs.input_stats.empty())
        return Cost{.work = inputs.config.unknown_leaf_cost};

    /// Non-leaf steps without a specific model (e.g. `LimitStep`, `BuildRuntimeFilterStep`) get zero
    /// local cost: they are cheap per row and identical across the alternatives being compared.
    return {};
}

String Cost::dump(const CostConfig & config) const
{
    return fmt::format("work={} network={} sequential={} total={}", work, network, sequential, total(config));
}

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    auto group = memo.getGroup(expression->group_id);

    /// Statistics must be derived before `estimateCost` is called.
    if (!group->statistics.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for group #{} (expression '{}') before estimateCost.\n"
            "Group state:\n{}",
            expression->group_id, expression->getDescription(), group->dump(memo.getEnvironment().cost_config));

    const Float64 distribution_node_count = static_cast<Float64>(std::max<size_t>(expression->properties.distribution.node_count, 1));
    /// Partitioned = 1/N per node; replicated = full work per node.
    const Float64 parallelism = expression->properties.distribution.is_replicated
        ? 1.0
        : distribution_node_count;

    /// Select the best implementation of each input first: the subtree cost accumulates over
    /// them, and an exchange prices its transfer on the selected child's physical output rows.
    std::vector<ExpressionWithCost> selected_inputs;
    selected_inputs.reserve(expression->inputs.size());
    bool has_unsatisfiable_input = false;
    for (const auto & input : expression->inputs)
    {
        ExpressionWithCost best;
        if (input.group_id == expression->group_id)
            /// Self-referential enforcer input: price it against an acyclic source (excluding
            /// itself), so the cost reflects a plan that can actually be built.
            best = memo.getGroup(input.group_id)->selectInputImplementation(
                input.required_properties, memo.getEnvironment().cost_config,
                std::unordered_set<GroupExpression *>{expression.get()}, /*input_is_self_referential=*/true);
        else
            best = memo.getGroup(input.group_id)->getBestImplementation(input.required_properties, memo.getEnvironment().cost_config);

        if (!best.expression)
            has_unsatisfiable_input = true;
        selected_inputs.push_back(std::move(best));
    }

    /// A selected child whose physical output differs from the group's logical statistics
    /// (a partial top-N emits up to L rows per node) overrides the exchange row count.
    std::optional<Float64> exchange_rows_override;
    if (dynamic_cast<const LogicalExchangeStep *>(expression->getQueryPlanStep())
        && !selected_inputs.empty() && selected_inputs[0].expression)
        exchange_rows_override = selected_inputs[0].expression->physical_output_rows;

    CostInputs inputs{
        .step = expression->getQueryPlanStep(),
        .output_stats = *group->statistics,
        .input_stats = {},
        .parallelism = parallelism,
        .node_count = distribution_node_count,
        .exchange_rows_override = exchange_rows_override,
        .config = memo.getEnvironment().cost_config,
    };
    inputs.input_stats.reserve(expression->inputs.size());
    for (const auto & input : expression->inputs)
    {
        auto input_group = memo.getGroup(input.group_id);
        inputs.input_stats.push_back(input_group->statistics ? &*input_group->statistics : nullptr);
    }

    /// A partial top-N emits at most L rows on each of its nodes, while the group statistics are
    /// trimmed to the final L. Record the physical output for parents; the input statistics are
    /// available here because costing runs after derivation.
    if (dynamic_cast<const PartialTopNStrategy *>(expression->strategy.get()))
    {
        const auto * sorting_step = typeid_cast<const SortingStep *>(expression->getQueryPlanStep());
        if (sorting_step && !inputs.input_stats.empty() && inputs.input_stats[0])
            expression->physical_output_rows = std::min(
                inputs.input_stats[0]->estimated_row_count,
                Float64(sorting_step->getLimit()) * distribution_node_count);
    }

    ExpressionCost total_cost;
    total_cost.cost = estimateOperatorCost(inputs, expression->strategy.get());
    total_cost.subtree_cost = total_cost.cost;

    /// An input with no implementation for its required properties means no plan can be
    /// built from this expression at all.
    if (has_unsatisfiable_input)
    {
        total_cost.subtree_cost = Cost::infinity();
        total_cost.buildable = false;
        LOG_TEST(log, "Cost of '{}': unbuildable, an input has no implementation for its required properties",
            expression->getName());
        return total_cost;
    }
    for (const auto & selected : selected_inputs)
        total_cost.subtree_cost += selected.cost.subtree_cost;

    LOG_TEST(log, "Cost of '{}': local {}; subtree {}",
        expression->getName(),
        total_cost.cost.dump(memo.getEnvironment().cost_config),
        total_cost.subtree_cost.dump(memo.getEnvironment().cost_config));
    return total_cost;
}


}
