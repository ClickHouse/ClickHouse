#include <Processors/QueryPlan/Optimizations/joinFilterPushDownPruningGate.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB::QueryPlanOptimizations
{

std::optional<PushDownPruningTarget> findPushDownPruningTarget(const QueryPlan::Node * node)
{
    PushDownPruningTarget target;
    while (node)
    {
        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        {
            target.reading = reading;
            return target;
        }
        if (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
        {
            /// Index analysis does not compose filters through arrayJoin, so pruning cannot happen
            if (expression->getExpression().hasArrayJoin())
                return {};
            target.expression_dags.push_back(&expression->getExpression());
        }
        else if (!typeid_cast<const FilterStep *>(node->step.get()))
        {
            return {};
        }
        if (node->children.size() != 1)
            return {};
        node = node->children.front();
    }
    return {};
}

std::optional<PushDownPruningTarget> findPruningTargetForColumn(const QueryPlan::Node * node, const std::string & column_name)
{
    if (auto direct = findPushDownPruningTarget(node))
        return direct;

    const bool is_join = typeid_cast<const JoinStepLogical *>(node->step.get())
        || typeid_cast<const JoinStep *>(node->step.get());
    if (!is_join)
        return {};

    for (const auto * join_child : node->children)
    {
        if (join_child->step->getOutputHeader()->has(column_name))
            return findPushDownPruningTarget(join_child);
    }
    return {};
}

bool pushedPredicateHelpsPruning(ActionsDAG candidate_dag, const PushDownPruningTarget & target)
{
    const std::string filter_name = candidate_dag.getOutputs().front()->result_name;
    for (const auto * expression_dag : target.expression_dags)
        candidate_dag = ActionsDAG::merge(expression_dag->clone(), std::move(candidate_dag));

    const auto * predicate = candidate_dag.tryFindInOutputs(filter_name);
    if (!predicate)
        return false;
    /// buildIndexes reads the predicate from the first output
    candidate_dag.getOutputs() = {predicate};

    auto & reading = *target.reading;
    /// FINAL can suppress partition pruning (rows with the same sorting key may span parts);
    /// mirror that decision, otherwise a partition-key conjunct looks useful but never prunes
    reading.deferFiltersAfterFinalIfNeeded();
    std::optional<ReadFromMergeTree::Indexes> indexes;
    ReadFromMergeTree::buildIndexes(
        indexes,
        &candidate_dag,
        reading.getMergeTreeData(),
        reading.getParts(),
        /*vector_search_parameters=*/std::nullopt,
        /*top_k_filter_info=*/std::nullopt,
        reading.getContext(),
        reading.getQueryInfo(),
        reading.getStorageMetadata(),
        reading.getSkipPartitionPruning());
    if (!indexes)
        return false;

    if (!indexes->key_condition->generateUnsubstituted().alwaysUnknownOrTrue())
        return true;
    if (indexes->partition_pruner && !indexes->partition_pruner->isUseless())
        return true;
    if (indexes->minmax_idx_condition && !indexes->minmax_idx_condition->generateUnsubstituted().alwaysUnknownOrTrue())
        return true;
    /// useful_indices is a planning-time candidate list; the read path additionally rejects an
    /// index per part when it depends on a column updated on the fly. Require at least one
    /// (part, index) pair that survives that check, otherwise no granule would be pruned
    if (!indexes->skip_indexes.useful_indices.empty())
    {
        for (const auto & part_with_ranges : reading.getParts())
        {
            auto alter_conversions = MergeTreeData::getAlterConversionsForPart(
                part_with_ranges.data_part, reading.getMutationsSnapshot(), reading.getContext());
            const auto & updated_columns = alter_conversions->getAllUpdatedColumns();
            for (const auto & index_with_condition : indexes->skip_indexes.useful_indices)
            {
                if (MergeTreeDataSelectExecutor::canUseIndex(index_with_condition.index, reading.getStorageMetadata(), updated_columns))
                    return true;
            }
        }
    }
    /// pruning by _part / _partition_id and _part_offset virtual columns
    if (indexes->part_values.has_value())
        return true;
    if (indexes->part_offset_condition || indexes->total_offset_condition)
        return true;
    return false;
}

}
