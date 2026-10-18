#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerDefaults.h>
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
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/ScatterExchangeStep.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <base/types.h>

#include <algorithm>
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
        /// `cpu_weight` is accepted as a legacy alias for `work_weight`.
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

/// The rows an input physically emits: its logical rows times their copies.
static Float64 inputPhysicalRows(const CostInputs & inputs, size_t input_index)
{
    const Float64 copies = input_index < inputs.input_copies.size() ? inputs.input_copies[input_index] : 1.0;
    return inputStats(inputs, input_index).estimated_row_count * copies;
}

static Float64 outputPhysicalRows(const CostInputs & inputs)
{
    return inputs.output_stats.estimated_row_count * inputs.output_copies;
}

/// Hash join: left probe + right build + output. The build side is materialized fully per node
/// for a broadcast join and 1/N per node otherwise. Network is modeled by the exchange steps.
static Cost hashJoinCost(const CostInputs & inputs, bool is_broadcast)
{
    const auto & right_stats = inputStats(inputs, 1);

    Cost cost;
    cost.work = (inputPhysicalRows(inputs, 0) + outputPhysicalRows(inputs)) / inputs.parallelism;

    /// The build side is materialized fully on every node for a broadcast join (the nodes build
    /// concurrently) and 1/N per node otherwise. The build is work, not a sequential phase:
    /// `parallel_hash` shards the hash table across threads within a node.
    const Float64 build_rows = inputPhysicalRows(inputs, 1);
    const Float64 build_rows_per_node = is_broadcast ? build_rows : build_rows / inputs.parallelism;
    /// Hash table materialization: memory allocation + cache pressure.
    cost.work += inputs.config.hash_table_build_factor * build_rows_per_node
        + build_rows_per_node * right_stats.estimated_bytes_per_row;
    return cost;
}

/// One node reads the whole table.
static Cost fullReadCost(const CostInputs & inputs)
{
    /// Price the scan volume, not the output volume: a filter the primary key cannot prune
    /// leaves the whole granule selection to scan, and without this a replicated read of such
    /// a table would look orders of magnitude cheaper than the scan it does.
    if (inputs.output_stats.physical_read_bytes > 0)
        return Cost{.work = inputs.output_stats.physical_read_bytes};
    return Cost{.work = inputs.output_stats.estimated_row_count * inputs.output_stats.estimated_bytes_per_row};
}

/// Hash table build + probe + output materialization, divided when each node handles 1/N.
static Cost aggregationCost(const CostInputs & inputs, Float64 divide_by)
{
    /// A partial aggregation holds and emits its states, which can be more than the groups.
    const Float64 output_rows = outputPhysicalRows(inputs);
    Cost cost;
    cost.work = (output_rows + output_rows * inputs.output_stats.estimated_bytes_per_row + inputPhysicalRows(inputs, 0))
        / divide_by;
    return cost;
}

/// A `DISTINCT` inserts every input row into a hash set and stores every distinct row. The final one
/// deduplicates a single stream on each node (`DistinctStep` merges the streams into one), so its inserts are
/// also sequential there; a preliminary one deduplicates all streams in parallel.
static Cost distinctCost(const CostInputs & inputs, bool is_final)
{
    const Float64 input_rows = inputPhysicalRows(inputs, 0);
    Cost cost;
    cost.work = (input_rows + outputPhysicalRows(inputs) * inputs.output_stats.estimated_bytes_per_row)
        / inputs.parallelism;
    if (is_final)
        cost.sequential = inputs.config.merge_sequential_cost_per_row * input_rows / inputs.parallelism;
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

Cost LocalDistinctStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return distinctCost(inputs, /*is_final=*/true);
}

Cost ShuffleDistinctStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return distinctCost(inputs, /*is_final=*/true);
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

/// Per-row window work, 1/N per node when the data is split across N nodes. A window
/// emits one output row per input row and writes every row with the columns it appends.
static Cost windowCost(const CostInputs & inputs)
{
    Cost cost;
    cost.work = inputs.output_stats.estimated_row_count
        * (1.0 + inputs.output_stats.estimated_bytes_per_row) / inputs.parallelism;
    return cost;
}

Cost WindowStrategy::estimateOperatorCost(const CostInputs & inputs) const
{
    return windowCost(inputs);
}

static Cost mergingAggregatedCost(const CostInputs & inputs)
{
    Cost cost;
    cost.work = (inputs.output_stats.estimated_row_count
        + inputs.output_stats.estimated_row_count * inputs.output_stats.estimated_bytes_per_row
        + inputPhysicalRows(inputs, 0)) / inputs.parallelism;
    /// Sequential ~ output groups (hash table size). Penalizes gather-to-one-node
    /// merge for large outputs; bucket-level merge within a node is parallel.
    cost.sequential = inputs.output_stats.estimated_row_count / inputs.parallelism;
    return cost;
}

static Cost broadcastExchangeCost(const CostInputs & inputs)
{
    Cost cost;
    /// The network term is wall-clock per node, like the work term (which is divided by
    /// parallelism): every receiving node ingests the full payload, all of them concurrently,
    /// and the senders each ship their share of the copies in parallel. So one full payload
    /// is the per-node bound. A shuffle moves 1/N per node, so a broadcast still costs N times
    /// a shuffle of the same data.
    cost.network += outputPhysicalRows(inputs) * inputs.output_stats.estimated_bytes_per_row;
    cost.sequential += inputs.config.exchange_fixed_overhead;
    return cost;
}

static Cost exchangeCost(const CostInputs & inputs, const IQueryPlanStep & step)
{
    /// An exchange moves the rows its input emits, whose copies it passes on: a sorted gather over a partial
    /// top-N transfers min(input_rows, L * node_count) rows, not the group's trimmed L.
    const Float64 rows = outputPhysicalRows(inputs);
    Cost cost;
    /// The network term is wall-clock per node, like the work term. A shuffle (N->N) moves
    /// 1/N of the data per node, all nodes concurrently; without the division one node
    /// scanning the whole table is priced the same as the cluster shuffling it, and the
    /// optimizer keeps big scans on a single node.
    Float64 network_bytes = rows * inputs.output_stats.estimated_bytes_per_row;
    if (dynamic_cast<const ShuffleExchangeStep *>(&step))
        network_bytes /= std::max(1.0, inputs.node_count);
    cost.network += network_bytes;
    cost.sequential += inputs.config.exchange_fixed_overhead;
    /// Gather (N->1) and Scatter (1->N) funnel every row through a single node that
    /// sends or receives them sequentially, so their transfer stays undivided and each row
    /// pays the funnel cost. Without it a gather of a large input looks as cheap as a
    /// shuffle, so the optimizer distributes work (e.g. a sort) that should stay local.
    /// An unknown join carries a search value that is the product of its inputs and a stand-in
    /// selectivity, which can be far below its rows, even below one row; such a result is funnelled
    /// at the rows it cannot exceed, capped at the largest known leaf. Other unknown results keep
    /// their search value: an aggregation of a small input is small.
    /// A sorted gather over a partial top-N funnels only the consumed rows: the receiver pulls merged rows
    /// until the limit is satisfied and cancels the senders. The network term above keeps the shipped
    /// rows - the senders push eagerly until the cancel arrives.
    const auto * sorted_gather = dynamic_cast<const GatherExchangeStep *>(&step);
    if (sorted_gather && !sorted_gather->getMaintainSortDescription())
        sorted_gather = nullptr;
    const auto * input_sorting = inputs.first_input_step ? typeid_cast<const SortingStep *>(inputs.first_input_step) : nullptr;
    const bool consumed_by_limit = sorted_gather && input_sorting && input_sorting->isPartialTopN();
    if (dynamic_cast<const GatherExchangeStep *>(&step) || dynamic_cast<const ScatterExchangeStep *>(&step))
    {
        Float64 funnelled_rows = rows;
        if (inputs.output_stats.rows_unknown && inputs.output_stats.search_value_is_join_product)
            funnelled_rows = std::max(1.0, std::min(inputs.output_stats.max_row_count, inputs.unknown_exchange_rows));
        else if (consumed_by_limit)
            funnelled_rows = std::min(funnelled_rows, inputs.output_stats.estimated_row_count);
        cost.network += std::max(0.0, funnelled_rows - rows) * inputs.output_stats.estimated_bytes_per_row;
        cost.sequential += inputs.config.funnel_sequential_cost_per_row * funnelled_rows;
    }
    if (sorted_gather)
    {
        /// The receiver of a sorted gather merges the sorted bucket streams into one
        /// (`GatherReceiveStep`): a serial per-row merge on one node, like a full sort's
        /// final phase; without this charge a sorted gather would cost the same as an
        /// unordered one. The merge advances only while the consumer pulls, so the limit above a
        /// partial top-N stops it at the trimmed output rows - the group statistics, not the
        /// shipped `rows`.
        if (sorted_gather->getSourceBucketCount() > 1)
            cost.sequential += inputs.config.merge_sequential_cost_per_row
                * (consumed_by_limit ? inputs.output_stats.estimated_row_count : rows);
        /// The sender also merges when its node has several streams (`GatherSendStep`);
        /// a single-stream source ships as is. The senders merge concurrently, so each
        /// one sees its node's share of the rows.
        if (inputs.first_input_properties
            && inputs.first_input_properties->stream_layout != StreamLayout::Single)
            cost.sequential += inputs.config.merge_sequential_cost_per_row * rows
                / std::max(1.0, Float64(inputs.first_input_properties->distribution.node_count));
    }
    return cost;
}

static Cost sortCost(const CostInputs & inputs, const SortingStep & sorting_step)
{
    const Float64 output_rows = outputPhysicalRows(inputs);
    Float64 rows = output_rows;
    /// A bounded (top-N) sort scans all of its input rows, keeping only the best L; the group
    /// stats are already trimmed to L, so read the input cardinality from the input.
    if (sorting_step.getLimit() > 0)
        rows = inputPhysicalRows(inputs, 0);
    /// A top-N keeps a heap of at most L rows (n * log L); a full sort is n * log n.
    const Float64 sorted_rows = sorting_step.getLimit() > 0
        ? std::min(rows, Float64(sorting_step.getLimit()))
        : rows;
    Cost cost;
    cost.work += rows * std::max(1.0, std::log2(sorted_rows)) / inputs.parallelism;
    /// N-way merge is single-threaded and sees only the rows the sort emits. A partitioned
    /// sort keeps its per-partition streams instead of merging them into one, so it pays no
    /// merge.
    if (!sorting_step.hasPartitions())
        cost.sequential += inputs.config.merge_sequential_cost_per_row * output_rows / inputs.parallelism;
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
        return Cost{.work = inputs.config.expression_cost_per_row * inputPhysicalRows(inputs, 0) / inputs.parallelism};

    if (typeid_cast<const AggregatingStep *>(step))
    {
        if (const auto * aggregation_strategy = dynamic_cast<const IAggregationStrategy *>(strategy))
            return aggregation_strategy->estimateOperatorCost(inputs);
        /// An aggregation without a strategy (e.g. from `DefaultImplementation`): price as Local.
        return aggregationCost(inputs, 1.0);
    }

    if (const auto * distinct_step = typeid_cast<const DistinctStep *>(step))
    {
        if (const auto * distinct_strategy = dynamic_cast<const IDistinctStrategy *>(strategy))
            return distinct_strategy->estimateOperatorCost(inputs);
        return distinctCost(inputs, /*is_final=*/!distinct_step->isPreliminary());
    }

    if (typeid_cast<const MergingAggregatedStep *>(step))
        return mergingAggregatedCost(inputs);

    if (typeid_cast<const WindowStep *>(step))
    {
        if (const auto * window_strategy = dynamic_cast<const IWindowStrategy *>(strategy))
            return window_strategy->estimateOperatorCost(inputs);
        return windowCost(inputs);
    }

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

namespace
{

struct PhysicalCopies
{
    Float64 copies = 1.0;
    bool unknown = false;
};

/// Whether every partition of `distribution` keeps all rows equal on `keys` together: each partitioning column set
/// has one of the keys.
bool splitByKeys(const DistributionDescription & distribution, const Names & keys)
{
    const NameSet key_set(keys.begin(), keys.end());
    return !distribution.columns.empty()
        && std::ranges::all_of(distribution.columns, [&](const NameSet & equivalent_columns)
            {
                return std::ranges::any_of(equivalent_columns, [&](const String & name) { return key_set.contains(name); });
            });
}

/// The copies of the logical rows an implementation emits (see `GroupExpression::physical_copies`), from its step and
/// the copies its selected inputs emit. A partial operator on several nodes emits a row of its result once on every
/// node that has input rows for it, unless the input is replicated or split by its keys, which brings those rows to
/// one node; never more than its input rows. An operator that merges the copies (the merge of an aggregation, a final
/// `DISTINCT`, a limit) emits the logical result; a join emits the copies of both inputs multiplied; other operators
/// pass on the copies of their input.
PhysicalCopies derivePhysicalCopies(const GroupExpression & expression, const CostInputs & inputs, bool input_copies_unknown)
{
    const auto * step = expression.getQueryPlanStep();
    if (inputs.input_copies.empty() || inputs.input_stats.empty() || !inputs.input_stats[0])
        return {};
    const Float64 logical_rows = inputs.output_stats.estimated_row_count;
    const Float64 input_rows = inputs.input_stats[0]->estimated_row_count * inputs.input_copies[0];
    const auto * input_distribution = inputs.first_input_properties ? &inputs.first_input_properties->distribution : nullptr;
    /// Copies of a partial result with `keys` that the producers emit, as a share of `logical_rows`.
    const auto partial_copies = [&](const Names & keys, bool unknown)
    {
        const bool together = input_distribution && (input_distribution->is_replicated || splitByKeys(*input_distribution, keys));
        const Float64 producers = together ? 1.0 : inputs.node_count;
        const Float64 rows = std::min(input_rows, logical_rows * producers);
        return PhysicalCopies{.copies = logical_rows > 0 ? std::max(1.0, rows / logical_rows) : 1.0, .unknown = unknown};
    };

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        const auto & params = aggregating_step->getParams();
        if (aggregating_step->isFinal() || params.only_merge)
            return {};
        /// A partial aggregation emits a state per group on every producer that has rows of the group: one copy when the
        /// input is replicated or split by the grouping keys, otherwise between one and `min(N, G * producers) / G`,
        /// depending on where the rows of a group are placed, which is not known: a read of a table sorted by a column
        /// correlated with the keys keeps a group on few producers. As in other optimizers, the search value is one copy,
        /// the old assumption, and the physical output is unknown; a run-time switch to pass-through is the remedy for
        /// groups spread over all producers. Without keys a producer emits one state, so the count is known, but the search
        /// value stays one copy: with one state per producer, gathering the input to one node looks cheaper whenever the
        /// input estimate is below the node count, and an underestimated input then is aggregated on one node.
        const bool together = input_distribution && (input_distribution->is_replicated || splitByKeys(*input_distribution, params.keys));
        const bool ordinary = !params.keys.empty() && !aggregating_step->isGroupingSets() && !params.overflow_row;
        return {.copies = 1.0, .unknown = !(together && ordinary) || input_copies_unknown};
    }
    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step); sorting_step && sorting_step->isPartialTopN())
    {
        /// At most L rows on each node, of the L the group keeps.
        const Float64 rows = std::min(input_rows, Float64(sorting_step->getLimit()) * inputs.node_count);
        return {.copies = logical_rows > 0 ? std::max(1.0, rows / logical_rows) : 1.0, .unknown = input_copies_unknown};
    }
    if (const auto * distinct_step = typeid_cast<const DistinctStep *>(step))
    {
        if (!distinct_step->isPreliminary())
            return {};
        /// A preliminary `DISTINCT` deduplicates each stream of a node on its own, so the copies per node are not
        /// known; the copies per node are the search value.
        return partial_copies(distinct_step->getColumnNames(), /*unknown=*/ true);
    }
    if (typeid_cast<const MergingAggregatedStep *>(step) || typeid_cast<const LimitStep *>(step))
        return {};
    if (typeid_cast<const JoinStepLogical *>(step) && inputs.input_copies.size() == 2)
        return {.copies = inputs.input_copies[0] * inputs.input_copies[1], .unknown = input_copies_unknown};
    if (inputs.input_copies.size() == 1)
        return {.copies = inputs.input_copies[0], .unknown = input_copies_unknown};
    /// Several inputs of other operators (a union): their copies are not combined.
    const bool all_logical = std::ranges::all_of(inputs.input_copies, [](Float64 input_copies) { return input_copies == 1.0; });
    return {.copies = 1.0, .unknown = input_copies_unknown || !all_logical};
}

}

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    auto group = memo.getGroup(expression->group_id);

    /// Statistics must be derived before `estimateCost` is called.
    if (!group->statistics.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for group #{} (expression '{}') before estimateCost.\n"
            "Group state:\n{}",
            expression->group_id, expression->getDescription(), group->dump(memo.getContext().cost_config));

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
                input.required_properties, memo.getContext().cost_config,
                std::unordered_set<GroupExpression *>{expression.get()}, /*input_is_self_referential=*/true);
        else
            best = memo.getGroup(input.group_id)->getBestImplementation(input.required_properties, memo.getContext().cost_config);

        if (!best.expression)
            has_unsatisfiable_input = true;
        selected_inputs.push_back(std::move(best));
    }

    const auto * step = expression->getQueryPlanStep();
    CostInputs inputs{
        .step = step,
        .output_stats = *group->statistics,
        .input_stats = {},
        .parallelism = parallelism,
        .node_count = distribution_node_count,
        .input_copies = {},
        .output_copies = 1.0,
        .unknown_exchange_rows = memo.getLargestKnownLeafRows().value_or(CascadesDefaults::DEFAULT_UNKNOWN_READ_ROWS),
        .first_input_properties = selected_inputs.empty() || !selected_inputs[0].expression
            ? nullptr
            : &selected_inputs[0].expression->properties,
        .first_input_step = selected_inputs.empty() || !selected_inputs[0].expression
            ? nullptr
            : selected_inputs[0].expression->getQueryPlanStep(),
        .config = memo.getContext().cost_config,
    };
    inputs.input_stats.reserve(expression->inputs.size());
    for (const auto & input : expression->inputs)
    {
        auto input_group = memo.getGroup(input.group_id);
        inputs.input_stats.push_back(input_group->statistics ? &*input_group->statistics : nullptr);
    }
    bool input_copies_unknown = false;
    for (const auto & selected : selected_inputs)
    {
        inputs.input_copies.push_back(selected.expression ? selected.expression->physical_copies : 1.0);
        input_copies_unknown |= selected.expression && selected.expression->physical_copies_unknown;
    }
    const auto copies = derivePhysicalCopies(*expression, inputs, input_copies_unknown);
    expression->physical_copies = copies.copies;
    expression->physical_copies_unknown = copies.unknown;
    inputs.output_copies = copies.copies;

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
        total_cost.cost.dump(memo.getContext().cost_config),
        total_cost.subtree_cost.dump(memo.getContext().cost_config));
    return total_cost;
}


}
