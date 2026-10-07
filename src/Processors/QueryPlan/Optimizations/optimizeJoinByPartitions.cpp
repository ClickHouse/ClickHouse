#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/useDataParallelAggregation.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/TimezoneMixin.h>
#include <Common/logger_useful.h>

#include <map>
#include <set>

namespace DB::QueryPlanOptimizations
{

namespace
{

struct JoinSide
{
    ReadFromMergeTree * reading = nullptr;
    /// Computes the columns at the input of the join from the columns of the storage.
    ActionsDAG dag;
    bool builds_runtime_filter = false;
};

/// Only steps that keep every row in its stream are allowed between the reading and the join.
std::optional<JoinSide> findJoinSide(const QueryPlan::Node & node)
{
    auto * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        /// A read in order outputs sorted streams, which the groups of partitions would interleave,
        /// and a streaming read assigns partitions to streams on its own.
        if (reading->isQueryWithFinal() || reading->isParallelReadingEnabled() || reading->hasRequestedOutputPortLayout()
            || reading->readsInOrder() || reading->getQueryInfo().isStream()
            || reading->getStorageMetadata()->getPartitionKey().column_names.empty())
            return {};

        /// The primary-key layers of `optimizeJoinByShards` are another port layout.
        if (const auto & analysis_result = reading->getAnalyzedResult(); analysis_result && !analysis_result->split_parts.layers.empty())
            return {};

        const auto & prewhere_info = reading->getPrewhereInfo();
        return JoinSide{
            .reading = reading,
            .dag = prewhere_info ? prewhere_info->prewhere_actions.clone() : ActionsDAG(reading->getOutputHeader()->getColumnsWithTypeAndName())};
    }

    if (node.children.size() != 1)
        return {};

    const ActionsDAG * expression = nullptr;
    const bool is_runtime_filter = typeid_cast<const BuildRuntimeFilterStep *>(step);
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
        expression = &expression_step->getExpression();
    else if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        expression = &filter_step->getExpression();
    else if (!is_runtime_filter)
        return {};

    auto side = findJoinSide(*node.children.front());
    if (!side)
        return {};
    if (expression)
        side->dag.mergeInplace(expression->clone());
    side->builds_runtime_filter |= is_runtime_filter;
    return side;
}

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// Storage column name -> indexes of the join keys which are exactly this column.
std::unordered_map<std::string, std::set<size_t>> getColumnsUsedAsKeys(const ActionsDAG & dag, const Names & key_names)
{
    std::unordered_map<std::string, std::set<size_t>> result;
    for (size_t i = 0; i < key_names.size(); ++i)
    {
        const auto * key = dag.tryFindInOutputs(key_names[i]);
        if (!key)
            continue;
        key = skipAliases(key);
        if (key->type == ActionsDAG::ActionType::INPUT)
            result[key->result_name].insert(i);
    }
    return result;
}

bool hasImplicitTimeZone(const IDataType & type)
{
    const auto * time_zone = dynamic_cast<const TimezoneMixin *>(removeLowCardinalityAndNullable(type.getPtr()).get());
    return time_zone && !time_zone->hasExplicitTimeZone();
}

/// Whether two partition key expressions compute the same value from rows with equal join keys.
/// Every input column must be used as a join key as is, and the matching inputs of the two sides must be
/// the two sides of the same join key.
struct PartitionKeyComparator
{
    std::unordered_map<std::string, std::set<size_t>> lhs_keys;
    std::unordered_map<std::string, std::set<size_t>> rhs_keys;

    bool equals(const ActionsDAG::Node * lhs, const ActionsDAG::Node * rhs) const
    {
        lhs = skipAliases(lhs);
        rhs = skipAliases(rhs);

        /// Types are compared by name: `equals` ignores parameters which change the result of functions,
        /// such as the time zone of `DateTime`. The implicit time zone is the one of the session that created
        /// the table, so it may differ between the tables.
        if (lhs->type != rhs->type || lhs->result_type->getName() != rhs->result_type->getName() || lhs->children.size() != rhs->children.size()
            || hasImplicitTimeZone(*lhs->result_type))
            return false;

        switch (lhs->type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
                auto lhs_it = lhs_keys.find(lhs->result_name);
                auto rhs_it = rhs_keys.find(rhs->result_name);
                if (lhs_it == lhs_keys.end() || rhs_it == rhs_keys.end())
                    return false;
                return std::ranges::any_of(lhs_it->second, [&](size_t i) { return rhs_it->second.contains(i); });
            }
            case ActionsDAG::ActionType::COLUMN:
                return lhs->column && rhs->column && isColumnConst(*lhs->column) && isColumnConst(*rhs->column)
                    && (*lhs->column)[0] == (*rhs->column)[0];
            case ActionsDAG::ActionType::FUNCTION:
                if (lhs->function_base->getName() != rhs->function_base->getName())
                    return false;
                for (size_t i = 0; i < lhs->children.size(); ++i)
                    if (!equals(lhs->children[i], rhs->children[i]))
                        return false;
                return true;
            default:
                return false;
        }
    }
};

bool haveSamePartitionKeyFunctionOfJoinKeys(const JoinSide & lhs, const JoinSide & rhs, const TableJoin::JoinOnClause & clause)
{
    const auto & lhs_partition_key = lhs.reading->getStorageMetadata()->getPartitionKey();
    const auto & rhs_partition_key = rhs.reading->getStorageMetadata()->getPartitionKey();
    if (lhs_partition_key.column_names.size() != rhs_partition_key.column_names.size())
        return false;

    auto is_function_of_keys = [](const JoinSide & side, const KeyDescription & partition_key, const Names & key_names)
    {
        ActionsDAG::NodeRawConstPtrs key_nodes;
        for (const auto & name : key_names)
        {
            const auto * node = side.dag.tryFindInOutputs(name);
            if (!node)
                return false;
            key_nodes.push_back(node);
        }

        /// Only the calculation of the keys matters, filters over other columns may be non-deterministic.
        auto key_dag = ActionsDAG::cloneSubDAG(key_nodes, /*remove_aliases=*/false);
        return isPartitionKeyFunctionOfKeys(partition_key, key_dag, key_names);
    };

    if (!is_function_of_keys(lhs, lhs_partition_key, clause.key_names_left) || !is_function_of_keys(rhs, rhs_partition_key, clause.key_names_right))
        return false;

    PartitionKeyComparator comparator{
        .lhs_keys = getColumnsUsedAsKeys(lhs.dag, clause.key_names_left),
        .rhs_keys = getColumnsUsedAsKeys(rhs.dag, clause.key_names_right)};

    auto lhs_outputs = lhs_partition_key.expression->getActionsDAG().findInOutputs(lhs_partition_key.column_names);
    auto rhs_outputs = rhs_partition_key.expression->getActionsDAG().findInOutputs(rhs_partition_key.column_names);
    for (size_t i = 0; i < lhs_outputs.size(); ++i)
        if (!comparator.equals(lhs_outputs[i], rhs_outputs[i]))
            return false;

    return true;
}

/// Partition id -> number of marks to read.
std::map<String, size_t> getPartitionsToRead(ReadFromMergeTree & reading)
{
    auto analysis_result = reading.getOrCreateAnalyzedResult();
    reading.setAnalyzedResult(analysis_result);

    std::map<String, size_t> partitions;
    for (const auto & part : analysis_result->parts_with_ranges)
        partitions[part.data_part->info.getPartitionId()] += part.getMarksCount();
    return partitions;
}

/// Assigns each partition to the least loaded group, starting from the largest partitions.
std::vector<Strings> splitPartitionsIntoGroups(const std::map<String, size_t> & partitions, size_t num_groups)
{
    std::vector<std::pair<size_t, String>> by_size;
    for (const auto & [partition_id, marks] : partitions)
        by_size.emplace_back(marks, partition_id);
    std::ranges::sort(by_size, std::greater{});

    std::vector<Strings> groups(num_groups);
    std::vector<size_t> group_marks(num_groups);
    for (auto & [marks, partition_id] : by_size)
    {
        size_t group = std::ranges::min_element(group_marks) - group_marks.begin();
        group_marks[group] += marks;
        groups[group].push_back(std::move(partition_id));
    }

    for (auto & group : groups)
        std::ranges::sort(group);
    return groups;
}

void tryShardJoinByPartitions(QueryPlan::Node & node, JoinStep & join_step, size_t max_threads)
{
    const auto & join = join_step.getJoin();
    if (!typeid_cast<const HashJoin *>(join.get()) && !typeid_cast<const ConcurrentHashJoin *>(join.get()))
        return;
    if (!join->isCloneSupported() || !join_step.sharding.empty())
        return;

    const auto & table_join = join->getTableJoin();
    const auto kind = table_join.kind();
    const auto strictness = table_join.strictness();
    if (!isInner(kind) && !isLeft(kind) && !isRight(kind) && !isFull(kind))
        return;
    if (strictness == JoinStrictness::Asof || table_join.getClauses().size() != 1)
        return;

    /// With swapped streams the left side of the `TableJoin` is the second child.
    size_t lhs_child = join_step.swap_streams ? 1 : 0;
    auto lhs = findJoinSide(*node.children[lhs_child]);
    if (!lhs)
        return;
    auto rhs = findJoinSide(*node.children[1 - lhs_child]);
    if (!rhs)
        return;

    /// Measured to be slower than a single `parallel_hash` join: with a runtime filter, which prunes the left side
    /// before the join, and for LEFT joins.
    if (typeid_cast<const ConcurrentHashJoin *>(join.get()) && (rhs->builds_runtime_filter || isLeft(kind)))
        return;

    const auto & clause = table_join.getClauses().front();
    if (!haveSamePartitionKeyFunctionOfJoinKeys(*lhs, *rhs, clause))
        return;

    auto lhs_partitions = getPartitionsToRead(*lhs->reading);
    auto rhs_partitions = getPartitionsToRead(*rhs->reading);

    /// Rows of a partition present on one side only have no match, so the partition is needed only if its
    /// unmatched rows are a part of the result.
    const bool keep_lhs = isLeftOrFull(kind) && strictness != JoinStrictness::Semi;
    const bool keep_rhs = isRightOrFull(kind) && strictness != JoinStrictness::Semi;
    std::map<String, size_t> partitions;
    for (const auto & [partition_id, marks] : lhs_partitions)
        if (keep_lhs || rhs_partitions.contains(partition_id))
            partitions[partition_id] += marks;
    for (const auto & [partition_id, marks] : rhs_partitions)
        if (keep_rhs || lhs_partitions.contains(partition_id))
            partitions[partition_id] += marks;

    /// Each group is read, built and probed by a single thread.
    if (partitions.size() < std::max<size_t>(2, max_threads / 2))
    {
        LOG_TRACE(getLogger("optimizeJoinByPartitions"), "Too few partitions to join by partitions: {}", partitions.size());
        return;
    }

    auto groups = splitPartitionsIntoGroups(partitions, std::min(max_threads, partitions.size()));
    lhs->reading->requestOutputPartitionGroupsThroughSeparatePorts(groups);
    rhs->reading->requestOutputPartitionGroupsThroughSeparatePorts(groups);

    JoinStep::JoinSharding sharding;
    sharding.kind = JoinStep::JoinSharding::Kind::Partitions;
    sharding.build_all_shards_before_probing = rhs->builds_runtime_filter;
    for (size_t i = 0; i < clause.key_names_left.size(); ++i)
        sharding.emplace_back(clause.key_names_left[i], clause.key_names_right[i]);
    join_step.enableJoinByLayers(std::move(sharding));
}

}

void optimizeJoinByPartitions(QueryPlan::Node & root, size_t max_threads)
{
    if (max_threads < 2)
        return;

    std::vector<QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (auto * join_step = typeid_cast<JoinStep *>(node->step.get()); join_step && node->children.size() == 2)
            tryShardJoinByPartitions(*node, *join_step, max_threads);

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

}
