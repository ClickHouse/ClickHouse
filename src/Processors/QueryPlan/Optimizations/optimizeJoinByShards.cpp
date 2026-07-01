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
#include <Core/Block.h>
#include <DataTypes/IDataType.h>

namespace DB
{
namespace QueryPlanOptimizations
{

static ReadFromMergeTree * findReadingStep(const QueryPlan::Node & node)
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

static ActionsDAG makeSourceDAG(ReadFromMergeTree & source)
{
    if (const auto & prewhere_info = source.getPrewhereInfo())
        return prewhere_info->prewhere_actions.clone();

    return ActionsDAG(source.getOutputHeader()->getColumnsWithTypeAndName());
}

/// This function builds a common DAG which is a merge of DAGs from Filter and Expression steps chain.
static bool updateDAG(const QueryPlan::Node & node, ActionsDAG & dag)
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
static JoinStep::PrimaryKeySharding findCommonPrimaryKeyPrefixByJoinKey(
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

    bool first = true;
    for (size_t pos = 0; pos < lhs_pk_colum_names.size() && pos < rhs_pk_colum_names.size(); ++pos)
    {
        bool ldesc = (pos < lhs_pk.reverse_flags.size()) ? lhs_pk.reverse_flags[pos] : false;
        bool rdesc = (pos < rhs_pk.reverse_flags.size()) ? rhs_pk.reverse_flags[pos] : false;
        if (ldesc != rdesc)
            break;

        if (first)
        {
            first = false;
            sharding.is_reverse_order = ldesc;
        }
        else
        {
            if (sharding.is_reverse_order != ldesc)
                break;
        }

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

/// We can apply sharding for multiple join steps at the same time.
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
    /// Whether the common primary key prefix used for sharding is in reverse order.
    bool is_reverse_order = false;
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
        /// Renumber part_index_in_query to be contiguous starting from added_parts.
        /// filterPartsByQueryConditionCache may drop parts from selectRangesToRead(),
        /// leaving non-contiguous part_index_in_query values. The distribution logic
        /// below assumes contiguous indices to assign parts back to their sources.
        for (size_t local_idx = 0; local_idx < analysis_result->parts_with_ranges.size(); ++local_idx)
        {
            all_parts.push_back(analysis_result->parts_with_ranges[local_idx]);
            all_parts.back().part_index_in_query = added_parts + local_idx;
        }

        analysis_results.push_back(std::move(analysis_result));
    }

    /// Split all the parts by layers.
    /// Generally, it should work because we don't use the part a lot. The only needed info is the PK prefix.
    /// The types of PK prefix expression always match because JOIN equality is applied to identical types (after conversions).
    auto logger = getLogger("optimizeJoinByLayers");
    auto all_split = splitIntersectingPartsRangesIntoLayers(
        all_parts, data.sources.front()->getNumStreams(), data.common_prefix, data.is_reverse_order, logger);
    std::vector<SplitPartsByRanges> splits(analysis_results.size());
    splits[0].borders = std::move(all_split.borders);
    splits[0].in_reverse_order = data.is_reverse_order;
    for (size_t i = 1; i < splits.size(); ++i)
    {
        splits[i].borders = splits[0].borders;
        splits[i].in_reverse_order = splits[0].in_reverse_order;
    }

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

    /// Do not break shards after full_sorting_merge JOIN. For hash join it is automatically true.
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
/// * no steps from source to JOIN can break sharding
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

                result->joins.is_reverse_order = sharding.is_reverse_order;
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
            /// The SortingStep can potentially appear from ORDER BY,
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

/// The shard is selected by the hash of the join key's byte representation
/// (`ScatterByPartitionTransform` -> `IColumn::computeHashInto`), while `FullSortingMergeJoin` matches keys
/// with `compareAt`. Some types make the two disagree: keys that `compareAt` treats as equal can hash
/// differently, so hash sharding would scatter them into different shards and the per-shard merge join would
/// never see the match, returning fewer rows than the `full_sorting_merge` algorithm this one is documented
/// to mirror. Two known cases:
///   - Floating-point: `-0.0` and `+0.0` (and NaNs) compare equal for the merge but their bit patterns hash
///     differently.
///   - `Object('json')` / `JSON`: `ColumnObject::compareAt` compares the logical path/value map, but the
///     hash depends on the physical layout - whether a given path is stored as a typed/dynamic subcolumn or
///     serialized into `shared_data`. That layout can differ between blocks (e.g. when the set of dynamic
///     paths differs across parts), so two logically-equal objects can hash into different shards.
/// This detects such a type at the top level or nested inside `Nullable`/`LowCardinality`/`Array`/`Tuple`/
/// `Map` (`Dynamic` keys are already rejected earlier by `TableJoin::inferJoinKeyCommonType`).
static bool joinKeyTypeBreaksHashSharding(const IDataType & type)
{
    auto breaks_sharding = [](const IDataType & t)
    {
        WhichDataType which(t);
        return which.isFloat() || which.isObject();
    };

    if (breaks_sharding(type))
        return true;

    bool result = false;
    type.forEachChild([&](const IDataType & child)
    {
        if (breaks_sharding(child))
            result = true;
    });
    return result;
}

/// Shard a `parallel_full_sorting_merge` join into independent per-shard merge joins by the hash of the
/// join keys.
///
/// Unlike `optimizeJoinByShards` above (which shards by primary-key ranges and only works when both sides
/// read from MergeTree in order), this works on any sorted input: each side's pre-join `SortingStep` is
/// switched to scatter the rows by the hash of the join keys into independent partitions and sort each
/// partition (one sorted stream per shard), and the join is executed shard-by-shard
/// (`JoinStep::enableJoinByLayers` -> `joinPipelinesYShapedByShards`). Because the partitioning depends only
/// on the join-key values (and the key types match - `FullSortingMergeJoin` requires it), equal keys land
/// in the same shard on both sides. The join output is unordered.
void optimizeParallelFullSortingMergeJoin(QueryPlan::Node & root, size_t num_shards)
{
    /// Need at least two shards to gain anything; with one shard this is a plain single merge join.
    if (num_shards <= 1)
        return;

    std::stack<QueryPlan::Node *> stack;
    stack.push(&root);

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (auto * join_step = typeid_cast<JoinStep *>(node->step.get());
            join_step && node->children.size() == 2)
        {
            const auto & join = join_step->getJoin();
            const auto & table_join = join->getTableJoin();

            /// Only shard when `parallel_full_sorting_merge` was the algorithm actually selected. Because
            /// both `full_sorting_merge` and `parallel_full_sorting_merge` build the same
            /// `FullSortingMergeJoin`, `join_algorithm` membership is not enough: e.g. with
            /// `full_sorting_merge,parallel_full_sorting_merge` the priority list selects plain
            /// `full_sorting_merge` first and the parallel variant is only an unreached fallback, so the
            /// sharded (unordered) rewrite must not fire. `FullSortingMergeJoin::isParallel` carries the
            /// selected algorithm from `chooseJoinAlgorithm`.
            ///
            /// `FullSortingMergeJoin` also serves `ASOF` joins, but they cannot be sharded by the hash of
            /// the whole key list: the trailing key is the inequality (`ASOF`) key, so rows with the same
            /// equality keys but different `ASOF` values would hash into different shards and the per-shard
            /// merge join could miss the closest match. The primary-key-range sharding path
            /// (`optimizeJoinByShards`) excludes `ASOF` for the same reason.
            const auto * full_sorting_merge_join = typeid_cast<const FullSortingMergeJoin *>(join.get());
            if (full_sorting_merge_join
                && full_sorting_merge_join->isParallel()
                && table_join.strictness() != JoinStrictness::Asof
                && table_join.getClauses().size() == 1)
            {
                auto * left_sort = typeid_cast<SortingStep *>(node->children[0]->step.get());
                auto * right_sort = typeid_cast<SortingStep *>(node->children[1]->step.get());

                /// Only rewrite plain full sorts. `applyOrder` (read-in-order), which runs before this pass,
                /// can turn a merge-join `SortingStep` into `FinishSorting` fed by an already in-order read
                /// (with `read_in_order_use_virtual_row = 1` the read even emits virtual-row chunks). The
                /// scatter path here is designed for a full sort of unsorted input, so re-routing such a
                /// read through `ScatterByPartitionTransform` throws away the pre-sortedness and mixes in
                /// virtual-row handling it does not expect. Sharding of in-order MergeTree reads is already
                /// handled by the primary-key-range path (`optimizeJoinByShards`), so leave these steps
                /// alone and let the join run as a single merge join.
                if (left_sort && right_sort
                    && left_sort->isSortingForMergeJoin() && right_sort->isSortingForMergeJoin()
                    && left_sort->getType() == SortingStep::Type::Full
                    && right_sort->getType() == SortingStep::Type::Full)
                {
                    const auto & clause = table_join.getClauses().front();
                    const auto & left_header = left_sort->getOutputHeader();
                    const auto & right_header = right_sort->getOutputHeader();

                    /// Do not shard when a join key is (or contains) a floating-point type: its hash-based
                    /// shard selection is not consistent with the merge-join `compareAt` (`-0.0` == `+0.0`,
                    /// NaN == NaN), so equal keys could land in different shards and the match would be lost
                    /// (see `joinKeyTypeBreaksHashSharding`). If a key column cannot be found to check its
                    /// type, be conservative and skip sharding as well. The join then runs as a single merge
                    /// join, exactly like `full_sorting_merge`.
                    bool can_shard = left_header && right_header;
                    for (size_t i = 0; can_shard && i < clause.key_names_left.size(); ++i)
                    {
                        const auto * left_key = left_header->findByName(clause.key_names_left[i]);
                        const auto * right_key = right_header->findByName(clause.key_names_right[i]);
                        if (!left_key || !right_key
                            || joinKeyTypeBreaksHashSharding(*left_key->type)
                            || joinKeyTypeBreaksHashSharding(*right_key->type))
                            can_shard = false;
                    }

                    if (can_shard)
                    {
                        left_sort->convertToScatteredFullSort(num_shards);
                        right_sort->convertToScatteredFullSort(num_shards);

                        JoinStep::PrimaryKeySharding sharding;
                        for (size_t i = 0; i < clause.key_names_left.size(); ++i)
                            sharding.emplace_back(clause.key_names_left[i], clause.key_names_right[i]);
                        join_step->enableJoinByLayers(std::move(sharding));
                    }
                }
            }
        }

        for (auto * child : node->children)
            stack.push(child);
    }
}

}
}
