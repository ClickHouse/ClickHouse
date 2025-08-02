#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB
{
namespace QueryPlanOptimizations
{

ReadFromMergeTree * findReadingStep(const QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (reading->isQueryWithFinal())
            return nullptr;

        if (reading->isParallelReadingEnabled())
            return nullptr;

        return reading;
    }

    return nullptr;
}

ActionsDAG makeSourceDAG(ReadFromMergeTree & source)
{
    if (const auto & prewhere_info = source.getPrewhereInfo())
        return prewhere_info->prewhere_actions.clone();

    return ActionsDAG(source.getOutputHeader().getColumnsWithTypeAndName());
}

/// This function builds a common DAG which is a merge of DAGs from Filter and Expression steps chain.
bool updateDAG(const QueryPlan::Node & node, ActionsDAG & dag)
{
    if (node.children.size() != 1)
        return false;

    IQueryPlanStep * step = node.step.get();

    if (typeid_cast<DistinctStep *>(step))
        return true;

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        dag.mergeInplace(expression->getExpression().clone());
        return true;
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        dag.mergeInplace(filter->getExpression().clone());
        return true;
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        const auto & array_joined_columns = array_join->getColumns();

        std::unordered_set<std::string_view> keys_set(array_joined_columns.begin(), array_joined_columns.end());

        /// Remove array joined columns from outputs.
        /// Types are changed after ARRAY JOIN, and we can't use this columns anyway.
        ActionsDAG::NodeRawConstPtrs outputs;
        outputs.reserve(dag.getOutputs().size());

        for (const auto & output : dag.getOutputs())
        {
            if (!keys_set.contains(output->result_name))
                outputs.push_back(output);
        }

        dag.getOutputs() = std::move(outputs);
        return true;
    }

    return false;
}

/// This function finds the common prefix of PK for left and right tables,
/// which is also used in JOIN equality condition.
///
/// Only the prefix size is needed, but here we additionally return names for debugging.
JoinStep::PrimaryKeySharding findCommonPrimaryKeyPrefixByJoinKey(
    ReadFromMergeTree * lhs_reading, const ActionsDAG & lhs_dag,
    ReadFromMergeTree * rhs_reading, const ActionsDAG & rhs_dag,
    const TableJoin::JoinOnClause & clause)
{
    const auto & lhs_pk = lhs_reading->getStorageMetadata()->getPrimaryKey();
    if (lhs_pk.column_names.empty())
        return {};

    const auto & rhs_pk = rhs_reading->getStorageMetadata()->getPrimaryKey();
    if (rhs_pk.column_names.empty())
        return {};

    std::unordered_map<std::string_view, const ActionsDAG::Node *> lhs_outputs;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> rhs_outputs;

    for (const auto & output : lhs_dag.getOutputs())
        lhs_outputs.emplace(output->result_name, output);
    for (const auto & output : rhs_dag.getOutputs())
        rhs_outputs.emplace(output->result_name, output);

    /// Here we match the DAG of PK and the DAG which is used in the query.
    const auto & lhs_pk_dag = lhs_pk.expression->getActionsDAG();
    const auto & lhs_pk_colum_names = lhs_pk.column_names;
    auto lhs_matches = matchTrees(lhs_pk_dag.getOutputs(), lhs_dag, false);
    const auto & rhs_pk_dag = rhs_pk.expression->getActionsDAG();
    const auto & rhs_pk_colum_names = rhs_pk.column_names;
    auto rhs_matches = matchTrees(rhs_pk_dag.getOutputs(), rhs_dag, false);

    JoinStep::PrimaryKeySharding sharding;

    for (size_t pos = 0; pos < lhs_pk_colum_names.size() && pos < rhs_pk_colum_names.size(); ++pos)
    {
        const auto * lhs_pk_output = lhs_pk_dag.tryFindInOutputs(lhs_pk_colum_names[pos]);
        const auto * rhs_pk_output = rhs_pk_dag.tryFindInOutputs(rhs_pk_colum_names[pos]);

        /// This should never happen.
        if (!lhs_pk_output || !rhs_pk_output)
            break;

        size_t keys_size = clause.key_names_left.size();

        /// Check all the equality conditions
        for (size_t i = 0; i < keys_size && sharding.size() <= pos; ++i)
        {
            const auto & left_name = clause.key_names_left[i];
            const auto & right_name = clause.key_names_right[i];

            // std::cerr << left_name << ' ' << right_name << std::endl;

            auto it = lhs_outputs.find(left_name);
            auto jt = rhs_outputs.find(right_name);

            /// This ideally should not happen as well.
            if (it == lhs_outputs.end() || jt == rhs_outputs.end())
                continue;

            /// Check if both keys any match the PK expression.
            auto lhs_match = lhs_matches.find(it->second);
            auto rhs_match = rhs_matches.find(jt->second);
            if (lhs_match == lhs_matches.end() || rhs_match == rhs_matches.end())
                continue;

            /// Match wrapped into monotonic function is not supported,
            /// but strict monotonic functions should work.
            if (lhs_match->second.monotonicity || rhs_match->second.monotonicity)
                continue;

            /// Check if both keys matched exactly to expected PK nodes.
            if (lhs_match->second.node == lhs_pk_output && rhs_match->second.node == rhs_pk_output)
                sharding.emplace_back(lhs_pk_colum_names[pos], rhs_pk_colum_names[pos]);
        }

        if (sharding.size() <= pos)
            break;
    }

    return sharding;
}

/// We can apply shardin for multiple join steps at the same time.
/// We only need to check that sharding is the same for all the sources.
struct JoinsAndSourcesWithCommonPrimaryKeyPrefix
{
    /// Join step and sharding prefix which can be applied.
    struct JoinAndSharding
    {
        JoinStep * join;
        JoinStep::PrimaryKeySharding sharding;
    };

    std::list<JoinAndSharding> joins;
    /// Separately, store joins which use full sorting merge algorithm.
    /// We should mark this joins to keep the number of streams for the left table the same.
    std::list<JoinStep *> joins_to_keep_in_order;
    /// Source steps are kept according to in-order traverse.
    /// The important part that the first source is the left-most.
    std::list<ReadFromMergeTree *> sources;
    /// For sorting steps which are created for full sorting merge algorithm,
    /// We need to change the sorting mode to sort partitions independently.
    std::list<SortingStep *> sorting_steps;
    /// Apply the minimum prefix in case of multiple joins.
    size_t common_prefix = std::numeric_limits<size_t>::max();
};

/// Apply the sharding optimization for the chosen joins.
static void apply(struct JoinsAndSourcesWithCommonPrimaryKeyPrefix & data)
{
    // std::cerr << "... apply for prefix " << data.common_prefix << " and joins " << data.joins.size() << std::endl;

    if (data.common_prefix == 0 || data.joins.empty())
        return;

    /// Here we take all the parts from all the sources.
    /// Update part index to restore back the set of parts.
    RangesInDataParts all_parts;
    std::vector<ReadFromMergeTree::AnalysisResultPtr> analysis_results;
    for (auto & source : data.sources)
    {
        auto analysis_result = source->getAnalyzedResult();
        if (!analysis_result)
            analysis_result = source->selectRangesToRead();

        size_t added_parts = all_parts.size();
        for (const auto & part : analysis_result->parts_with_ranges)
        {
            all_parts.push_back(part);
            all_parts.back().part_index_in_query += added_parts;
        }

        analysis_results.push_back(std::move(analysis_result));
    }

    /// Split all the parts by layers.
    /// Generally, it should work because we don't use the part a lot. The only needed info is the PK prefix.
    /// The types of PK prefix expression always match because JOIN equality is applied to identical types (after conversions).
    auto logger = getLogger("optimizeJoinByLayers");
    auto all_split = splitIntersectingPartsRangesIntoLayers(all_parts, data.sources.front()->getNumStreams(), data.common_prefix, false, logger);
    std::vector<SplitPartsByRanges> splits(analysis_results.size());
    splits[0].borders = std::move(all_split.borders);
    for (size_t i = 1; i < splits.size(); ++i)
        splits[i].borders = splits[0].borders;

    /// After we got a layers, restore the part source back.
    for (auto & layer : all_split.layers)
    {
        std::sort(layer.begin(), layer.end(),
            [](const RangesInDataPart & lhs, const RangesInDataPart & rhs)
            { return lhs.part_index_in_query < rhs.part_index_in_query; });

        size_t next_part = 0;
        size_t sum_parts = 0;
        for (size_t i = 0; i < splits.size(); ++i)
        {
            auto & new_layer = splits[i].layers.emplace_back();
            size_t num_parts_in_source = analysis_results[i]->parts_with_ranges.size();
            while (next_part < layer.size() && layer[next_part].part_index_in_query < sum_parts + num_parts_in_source)
            {
                auto & new_part_range = new_layer.emplace_back(layer[next_part]);
                new_part_range.part_index_in_query -= sum_parts;
                ++next_part;
            }
            sum_parts += num_parts_in_source;
        }
    }

    /// Attach split parts to analysis result. Hopefully no other optimization would be done.
    for (size_t i = 0; i < splits.size(); ++i)
        analysis_results[i]->split_parts = std::move(splits[i]);

    /// Apply sharding to joins with the minimum prefix.
    for (auto & join_and_sharding : data.joins)
    {
        join_and_sharding.sharding.resize(data.common_prefix);
        join_and_sharding.join->enableJoinByLayers(std::move(join_and_sharding.sharding));
    }

    /// Apply sharding for JOIN sorting steps.
    for (const auto & sorting_step : data.sorting_steps)
        sorting_step->convertToPartitionedFinishSorting();

    /// Do not breake shards after full_sorting_merge JOIN. For hash join it is automatically true.
    for (const auto & join_step : data.joins_to_keep_in_order)
        join_step->keepLeftPipelineInOrder();
}

/// Join can be executed by independent shards if the same sharding can apply for the keft and right part,
/// and also the join condition can be applied within shard independently (only equality is supported).
///
/// In case if left and right tables are reading from MergeTree, we check for PK prefix
/// and enable the special reading mode where each output port correspond to the independent shard.
///
/// The case with multiple joins is supported.
/// Generally, we can apply sharding to JOIN if
/// * the leftmost source of the left subtree and the leftmost source of the right subtree is reading from MergerTree
/// * no steps from source to JOIN can breake sharding
///
/// The last criteria
/// * true for Expression, Filter, HashJoin steps
/// * can be enforced for fill_sorting_merge JOIN and Sorting step added for it
///
/// The algorithm finds as many JOIN steps as it can and apply optimization for the minimum possible prefix.
void optimizeJoinByShards(QueryPlan::Node & root)
{
    /// The algorithm is basically DFS which build the Result structure for the every child step,
    /// And then update the Result for the current step or apply the optimization.
    struct Result
    {
        JoinsAndSourcesWithCommonPrimaryKeyPrefix joins;
        /// For the leftmost source, we are building the DAG which represent expression execution.
        ActionsDAG dag;
    };

    struct Frame
    {
        const QueryPlan::Node * node;
        size_t next_child_to_process = 0;
        std::vector<std::optional<Result>> results{};
    };

    std::optional<Result> result;
    std::stack<Frame> stack;
    stack.push({&root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (frame.next_child_to_process > 0)
            frame.results.push_back(std::move(result));

        result = {};

        if (frame.next_child_to_process < frame.node->children.size())
        {
            stack.push({frame.node->children[frame.next_child_to_process]});
            ++frame.next_child_to_process;
            continue;
        }

        if (auto * join_step = typeid_cast<JoinStep *>(frame.node->step.get()))
        {
            const auto & join = join_step->getJoin();

            auto * hash_join = typeid_cast<HashJoin *>(join.get());
            auto * concurrent_hash_join = typeid_cast<ConcurrentHashJoin *>(join.get());
            auto * full_sorting_merge_join = typeid_cast<FullSortingMergeJoin *>(join.get());
            bool is_algo_supported = hash_join || concurrent_hash_join || full_sorting_merge_join;

            bool can_split_left_table = frame.results.front() != std::nullopt && is_algo_supported && !join->hasDelayedBlocks();
            // std::cerr << "can_split_left_table " << can_split_left_table << std::endl;

            const auto & table_join = join->getTableJoin();
            auto kind = table_join.kind();
            auto strictness = table_join.strictness();
            const auto & clauses = table_join.getClauses();

            bool can_split_join = frame.results.back() != std::nullopt && can_split_left_table
                && (isLeft(kind) || isRight(kind) || isInner(kind) || isFull(kind))
                && strictness != JoinStrictness::Asof
                && clauses.size() == 1;

            // std::cerr << "can_split_join " << can_split_join << std::endl;

            JoinStep::PrimaryKeySharding sharding;
            if (can_split_join)
            {
                // std::cerr << frame.results.front()->dag.dumpDAG() << std::endl;
                // std::cerr << frame.results.back()->dag.dumpDAG() << std::endl;

                /// Note: join_use_nulls is not supported.
                /// This is because we append toNullable function.
                /// We can remove this function from the DAG or mark is as identity later.

                sharding = findCommonPrimaryKeyPrefixByJoinKey(
                    frame.results.front()->joins.sources.front(), frame.results.front()->dag,
                    frame.results.back()->joins.sources.front(), frame.results.back()->dag,
                    clauses[0]);
            }

            // std::cerr << "common_prefix " << common_prefix << std::endl;

            if (!sharding.empty())
            {
                result = std::move(frame.results.front());

                /// Here we choose the minimal common prefix.
                /// Applying optimization to more joins is potentially better.
                /// Hopefully, even the first PK column would be enough to shard the data.
                result->joins.common_prefix = std::min(result->joins.common_prefix, sharding.size());
                result->joins.common_prefix = std::min(result->joins.common_prefix, frame.results.back()->joins.common_prefix);

                result->joins.joins.emplace_back(join_step, std::move(sharding));
                result->joins.joins.splice(result->joins.joins.end(), std::move(frame.results.back()->joins.joins));
                result->joins.sources.splice(result->joins.sources.end(), std::move(frame.results.back()->joins.sources));
                result->joins.sorting_steps.splice(result->joins.sorting_steps.end(), std::move(frame.results.back()->joins.sorting_steps));
                result->joins.joins_to_keep_in_order.splice(result->joins.joins_to_keep_in_order.end(), std::move(frame.results.back()->joins.joins_to_keep_in_order));

                frame.results.back() = std::nullopt;
            }
            else if (can_split_left_table)
            {
                /// TODO : check if any type conversion is needed for join_use_nulls.
                result = std::move(frame.results.front());
                result->joins.joins_to_keep_in_order.emplace_back(join_step);
            }
        }
        else if (typeid_cast<DelayedCreatingSetsStep *>(frame.node->step.get()))
        {
            result = std::move(frame.results.front());
        }
        else if (auto * source = findReadingStep(*frame.node))
        {
            result.emplace();
            result->joins.sources.emplace_back(source);
            result->dag = makeSourceDAG(*source);
        }
        else if (auto * sorting = typeid_cast<SortingStep *>(frame.node->step.get());
            sorting && sorting->isSortingForMergeJoin() && sorting->getType() == SortingStep::Type::FinishSorting)
        {
            /// Here we assume that read-in-order is applied for full sorting merge join.
            /// The SortingStep can potentially apper from ORDER BY,
            /// but it would be useless because JOIN does not enforce sorting by itself.

            if (frame.results.size() == 1 && frame.results[0])
            {
                result = std::move(frame.results[0]);
                result->joins.sorting_steps.push_back(sorting);
            }
        }
        else if (frame.results.size() == 1 && frame.results[0])
        {
            if (updateDAG(*frame.node, frame.results[0]->dag))
                result = std::move(frame.results[0]);
        }

        for (auto & cur_result : frame.results)
            if (cur_result)
                apply(cur_result->joins);

        stack.pop();
    }

    if (result)
        apply(result->joins);
}

}
}
