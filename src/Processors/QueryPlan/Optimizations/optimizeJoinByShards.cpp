#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/PartsSplitter.h>
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
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/KeyDescription.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <queue>

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

        /// A reading already claimed by the independent-partitions optimization (for DISTINCT,
        /// LIMIT BY or aggregation) outputs one port per partition, while reading by layers outputs
        /// one port per primary-key range. Both are contracts about which rows go to which output
        /// port, consumed positionally by the steps above, so a single read can satisfy only one of
        /// them: partitions generally do not form primary-key ranges, and there is no repartitioning
        /// step between the two consumers that could convert one port layout into the other.
        if (reading->willOutputEachPartitionThroughSeparatePort())
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
        /// The layer split compares key values as `greater(tuple(pk), tuple(border))`, and an IEEE
        /// comparison answers false for `NaN` against anything, so a row with a `NaN` key fails the
        /// filter of every layer - including the last one, which only has a lower bound - and is
        /// dropped at read time. `Null` and a `NaN` nested in a container compare inconsistently there
        /// for the same reason, which is why every other consumer of
        /// `splitIntersectingPartsRangesIntoLayers` gates on this predicate. Only the prefix the split
        /// actually reads has to be safe, so an unsafe column just ends the prefix here.
        if (!isSafePrimaryDataKeyType(*lhs_pk.data_types[pos]) || !isSafePrimaryDataKeyType(*rhs_pk.data_types[pos]))
            break;

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
    /// The `part_index_in_query` a part had in its source, by its position in `all_parts`.
    std::vector<size_t> original_part_indexes;
    std::vector<ReadFromMergeTree::AnalysisResultPtr> analysis_results;
    for (auto & source : data.sources)
    {
        auto analysis_result = source->getAnalyzedResult();
        if (!analysis_result)
            analysis_result = source->selectRangesToRead();

        size_t added_parts = all_parts.size();
        /// Renumber part_index_in_query to be contiguous starting from added_parts.
        /// Index analysis and filterPartsByQueryConditionCache may drop parts from selectRangesToRead(),
        /// leaving non-contiguous part_index_in_query values. The distribution logic
        /// below assumes contiguous indices to assign parts back to their sources.
        /// The original index is remembered: the read step keys its per-part state
        /// (the ranges read by the skip indexes, the `_part_index` virtual column) by it.
        for (size_t local_idx = 0; local_idx < analysis_result->parts_with_ranges.size(); ++local_idx)
        {
            all_parts.push_back(analysis_result->parts_with_ranges[local_idx]);
            original_part_indexes.push_back(all_parts.back().part_index_in_query);
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
                new_part_range.part_index_in_query = original_part_indexes[new_part_range.part_index_in_query];
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

/// Shard a `parallel_full_sorting_merge` join into independent per-shard merge joins by the hash of the
/// join keys.
///
/// Unlike `optimizeJoinByShards` above (which shards by primary-key ranges and only works when both sides
/// read from MergeTree in order), this works on any unsorted input: each side's pre-join full
/// `SortingStep` is switched to scatter the rows by the hash of the join keys into independent partitions
/// and sort each partition (one sorted stream per shard), and the join is executed shard-by-shard
/// (`JoinStep::enableJoinByLayers` -> `joinPipelinesYShapedByShards`). Because the partitioning depends only
/// on the join-key values (and equal values hash equally through `LowCardinality`/`Nullable` wrappers, per
/// `IColumn::computeHashInto`), equal keys land in the same shard on both sides. The join output is unordered.
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

            /// Only shard when `parallel_full_sorting_merge` was the algorithm actually selected. Both
            /// algorithms build the same `FullSortingMergeJoin`, so `join_algorithm` membership is not
            /// enough: with `full_sorting_merge,parallel_full_sorting_merge` the priority list selects plain
            /// `full_sorting_merge` and the parallel variant is an unreached fallback, so the sharded
            /// (unordered) rewrite must not fire. `FullSortingMergeJoin::isParallel` carries the selected
            /// algorithm over from `chooseJoinAlgorithm`.
            ///
            /// `ASOF` joins also use `FullSortingMergeJoin` but cannot be sharded by the hash of the whole
            /// key list: its trailing key is the inequality key, so rows with equal equality keys but
            /// different `ASOF` values would land in different shards and the closest match could be missed.
            /// The primary-key-range path (`optimizeJoinByShards`) excludes `ASOF` for the same reason.
            const auto * full_sorting_merge_join = typeid_cast<const FullSortingMergeJoin *>(join.get());
            if (full_sorting_merge_join
                && full_sorting_merge_join->isParallel()
                && table_join.strictness() != JoinStrictness::Asof
                && table_join.getClauses().size() == 1)
            {
                auto * left_sort = typeid_cast<SortingStep *>(node->children[0]->step.get());
                auto * right_sort = typeid_cast<SortingStep *>(node->children[1]->step.get());

                /// Only a plain full sort (`Type::Full`) is scattered: the input is unsorted, so each shard
                /// sorts from scratch (`convertToScatteredFullSort`). Losing the input order is safe - this
                /// sort exists only to feed the merge join, whose result is unordered anyway.
                ///
                /// An already-sorted side (`FinishSorting`: a MergeTree read in order, or any input
                /// recognized as pre-sorted) must NOT be scattered, tempting as an order-preserving scatter
                /// is. It can deadlock: a `ScatterByPartitionTransform` does not consume new input until all
                /// partition chunks of the previous one are pushed, while its consumers (per-partition
                /// `MergingSortedTransform`s, per-shard `MergeJoinTransform`s) wait for a chunk of one
                /// specific input each. Two such scatters then form a circular wait - A blocked pushing to
                /// shard `i` whose merge waits on B, B blocked pushing to shard `j` whose merge waits on A
                /// (seen as `Logical error: Pipeline stuck` in the AST fuzzer). The full-sort path is immune:
                /// each `MergeSortingTransform` drains its whole input before emitting anything.
                ///
                /// A pre-sorted side therefore runs as a single merge join, exactly like
                /// `full_sorting_merge`, keeping the in-order read and its virtual rows
                /// (`read_in_order_use_virtual_row`) intact. Such sides can still be joined shard-by-shard
                /// without a shuffle by the primary-key-range path (`optimizeJoinByShards`,
                /// `query_plan_join_shard_by_pk_ranges`), which splits both reads at the source.
                auto is_scatterable_merge_join_sort = [](const SortingStep & sort)
                {
                    return sort.isSortingForMergeJoin() && sort.getType() == SortingStep::Type::Full;
                };
                if (left_sort && right_sort
                    && is_scatterable_merge_join_sort(*left_sort)
                    && is_scatterable_merge_join_sort(*right_sort))
                {
                    const auto & clause = table_join.getClauses().front();
                    const auto & left_header = left_sort->getOutputHeader();
                    const auto & right_header = right_sort->getOutputHeader();

                    /// Do not shard when a join key is (or contains) a type whose hash-based shard selection
                    /// is not consistent with the merge-join `compareAt` - floating-point (`-0.0` == `+0.0`,
                    /// NaN == NaN), `JSON`/`Object`, or `Dynamic` - so equal keys could land in different
                    /// shards and the match would be lost (see `keyTypeBreaksHashSharding`). If a key
                    /// column cannot be found to check its type, be conservative and skip sharding as well.
                    /// The join then runs as a single merge join, exactly like `full_sorting_merge`.
                    bool can_shard = left_header && right_header;
                    for (size_t i = 0; can_shard && i < clause.key_names_left.size(); ++i)
                    {
                        const auto * left_key = left_header->findByName(clause.key_names_left[i]);
                        const auto * right_key = right_header->findByName(clause.key_names_right[i]);
                        if (!left_key || !right_key
                            || keyTypeBreaksHashSharding(*left_key->type)
                            || keyTypeBreaksHashSharding(*right_key->type))
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


/// Follows a join input down to the MergeTree read it consumes, through the steps which keep every row in
/// its stream and keep the join key columns, so that the output ports of the read reach the join as they
/// are. On success `dag` maps the columns of the read to the columns of the join input.
static ReadFromMergeTree * findReadingForJoinByPartitions(const QueryPlan::Node & node, std::optional<ActionsDAG> & dag)
{
    if (auto * reading = findReadingStep(node))
    {
        /// Already split into primary-key layers by `optimizeJoinByShards`.
        if (const auto & analysis = reading->getAnalyzedResult(); analysis && !analysis->split_parts.layers.empty())
            return nullptr;

        /// These reads do not read by layers: a distributed worker reads its bucket of marks, a streaming
        /// read groups the parts itself (and the parts it reads later would not be in any layer).
        if (reading->getDistributedReadBucketCount() > 0 || reading->getQueryInfo().isStream())
            return nullptr;

        dag = makeSourceDAG(*reading);
        return reading;
    }

    if (node.children.size() != 1)
        return nullptr;

    const auto * step = node.step.get();
    bool is_expression = typeid_cast<const ExpressionStep *>(step) || typeid_cast<const FilterStep *>(step);
    /// Only collects the keys of every stream into the runtime filter, the rows pass through.
    bool is_passthrough = typeid_cast<const BuildRuntimeFilterStep *>(step);
    if (!is_expression && !is_passthrough)
        return nullptr;

    auto * reading = findReadingForJoinByPartitions(*node.children.front(), dag);
    if (reading && is_expression)
        updateDAG(node, *dag);
    return reading;
}

/// The column of the read which the join key passes through unchanged, or nullptr.
static const ActionsDAG::Node * findReadColumnOfKey(const ActionsDAG & dag, const String & key_name)
{
    const auto * node = dag.tryFindInOutputs(key_name);
    while (node && node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node && node->type == ActionsDAG::ActionType::INPUT ? node : nullptr;
}

static bool hasImplicitTimeZone(const IDataType & type)
{
    if (const auto * date_time = typeid_cast<const DataTypeDateTime *>(&type))
        return !date_time->hasExplicitTimeZone();
    if (const auto * date_time_64 = typeid_cast<const DataTypeDateTime64 *>(&type))
        return !date_time_64->hasExplicitTimeZone();
    return false;
}

/// Whether the partition key computes a value in a `DateTime` without an explicit time zone. Such a type
/// takes the time zone in effect when the table was created or loaded, which is not visible in its name,
/// so two tables with the same expression can put the same value into different partitions.
static bool partitionKeyDependsOnImplicitTimeZone(const KeyDescription & partition_key)
{
    for (const auto & node : partition_key.expression->getActionsDAG().getNodes())
    {
        bool found = hasImplicitTimeZone(*node.result_type);
        node.result_type->forEachChild([&](const IDataType & child) { found = found || hasImplicitTimeZone(child); });
        if (found)
            return true;
    }
    return false;
}

/// Returns the partition key expression of one join side with every column replaced by a placeholder
/// naming the position and the type of the join key equal to that column, or nullptr if the partition
/// key uses a column which is not a join key. If both sides give the same expression, rows with equal
/// join keys have equal partition values, hence equal partition IDs, in both tables.
///
/// The types are part of the placeholder because the same expression can map equal values of different
/// types to different partitions, e.g. `toYYYYMM` of a `DateTime` in different time zones.
static ASTPtr canonicalizePartitionKey(
    const KeyDescription & partition_key, const ActionsDAG & dag, const Names & key_names, std::vector<size_t> & used_keys)
{
    if (partition_key.column_names.empty() || !partition_key.expression_list_ast
        || partitionKeyDependsOnImplicitTimeZone(partition_key))
        return nullptr;

    const auto storage_columns = partition_key.expression->getRequiredColumnsWithTypes();

    std::unordered_map<String, std::pair<size_t, String>> key_of_column;
    for (size_t i = 0; i < key_names.size(); ++i)
    {
        const auto * column = findReadColumnOfKey(dag, key_names[i]);
        if (!column)
            continue;

        /// The read must return the column in the type the partition value was computed from on insert.
        auto storage_column = storage_columns.tryGetByName(column->result_name);
        if (!storage_column || storage_column->type->getName() != column->result_type->getName())
            continue;

        key_of_column.emplace(column->result_name, std::pair{i, fmt::format("__join_key_{}_{}", i, column->result_type->getName())});
    }

    auto ast = partition_key.expression_list_ast->clone();
    bool all_columns_are_keys = true;
    std::function<void(IAST &)> replace_columns = [&](IAST & node)
    {
        if (auto * identifier = node.as<ASTIdentifier>())
        {
            auto it = key_of_column.find(identifier->name());
            if (it == key_of_column.end())
            {
                all_columns_are_keys = false;
                return;
            }

            identifier->setShortName(it->second.second);
            used_keys.push_back(it->second.first);
            return;
        }

        for (const auto & child : node.children)
            replace_columns(*child);
    };
    replace_columns(*ast);

    if (!all_columns_are_keys)
        return nullptr;

    return ast;
}

static void tryJoinByPartitions(QueryPlan::Node & node, JoinStep & join_step)
{
    const auto & join = join_step.getJoin();
    if (!typeid_cast<const HashJoin *>(join.get()) && !typeid_cast<const ConcurrentHashJoin *>(join.get()))
        return;

    if (join->hasDelayedBlocks() || !join->isCloneSupported())
        return;

    const auto & table_join = join->getTableJoin();
    auto kind = table_join.kind();
    if (!(isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind))
        || table_join.strictness() == JoinStrictness::Asof
        || table_join.getClauses().size() != 1)
        return;

    const auto & clause = table_join.getClauses().front();

    /// The inputs in the order of the sides of `table_join`.
    size_t left_input = join_step.areInputsSwapped() ? 1 : 0;
    std::optional<ActionsDAG> left_dag;
    std::optional<ActionsDAG> right_dag;
    auto * left_reading = findReadingForJoinByPartitions(*node.children[left_input], left_dag);
    auto * right_reading = findReadingForJoinByPartitions(*node.children[1 - left_input], right_dag);
    if (!left_reading || !right_reading)
        return;

    std::vector<size_t> used_keys;
    std::vector<size_t> right_used_keys;
    auto left_partition_key = canonicalizePartitionKey(
        left_reading->getStorageMetadata()->getPartitionKey(), *left_dag, clause.key_names_left, used_keys);
    auto right_partition_key = canonicalizePartitionKey(
        right_reading->getStorageMetadata()->getPartitionKey(), *right_dag, clause.key_names_right, right_used_keys);
    if (!left_partition_key || !right_partition_key || used_keys.empty()
        || left_partition_key->getTreeHash(/*ignore_aliases=*/ true) != right_partition_key->getTreeHash(/*ignore_aliases=*/ true))
        return;

    auto get_analysis = [](ReadFromMergeTree & reading)
    {
        auto analysis = reading.getAnalyzedResult();
        return analysis ? analysis : reading.selectRangesToRead();
    };
    auto left_analysis = get_analysis(*left_reading);
    auto right_analysis = get_analysis(*right_reading);

    auto marks_by_partition = [](const ReadFromMergeTree::AnalysisResult & analysis)
    {
        std::map<String, size_t> marks;
        for (const auto & part : analysis.parts_with_ranges)
            marks[part.data_part->info.getPartitionId()] += part.getMarksCount();
        return marks;
    };
    auto left_partitions = marks_by_partition(*left_analysis);
    auto right_partitions = marks_by_partition(*right_analysis);

    /// A partition read by one side only has no rows to match, it is needed only for the non-matched
    /// rows of that side.
    std::map<String, size_t> partitions;
    for (const auto & [partition_id, marks] : left_partitions)
        if (isLeftOrFull(kind) || right_partitions.contains(partition_id))
            partitions[partition_id] += marks;
    for (const auto & [partition_id, marks] : right_partitions)
        if (isRightOrFull(kind) || left_partitions.contains(partition_id))
            partitions[partition_id] += marks;

    size_t num_layers = std::min(partitions.size(), left_reading->getNumStreams());
    if (num_layers < 2)
        return;

    /// Balance the layers: the heaviest partition goes to the least loaded layer first.
    std::vector<std::pair<size_t, String>> partitions_by_marks;
    for (const auto & [partition_id, marks] : partitions)
        partitions_by_marks.emplace_back(marks, partition_id);
    std::ranges::sort(partitions_by_marks, std::greater{});

    using LayerLoad = std::pair<size_t, size_t>;
    std::priority_queue<LayerLoad, std::vector<LayerLoad>, std::greater<>> layer_loads;
    for (size_t layer = 0; layer < num_layers; ++layer)
        layer_loads.emplace(0, layer);

    std::unordered_map<String, size_t> layer_of_partition;
    for (const auto & [marks, partition_id] : partitions_by_marks)
    {
        auto [load, layer] = layer_loads.top();
        layer_loads.pop();
        layer_of_partition.emplace(partition_id, layer);
        layer_loads.emplace(load + marks, layer);
    }

    /// Both reads output one port per layer, and port `i` of both carries the same partitions.
    /// A layer without parts on one side still occupies its port (see `getNumStreamsWhenNothingToRead`).
    auto split_by_layers = [&](ReadFromMergeTree & reading, const ReadFromMergeTree::AnalysisResult & analysis)
    {
        /// A copy, the analysis result may be shared with another read of the same table.
        auto result = std::make_shared<ReadFromMergeTree::AnalysisResult>(analysis);
        result->split_parts = {};
        result->split_parts.layers.resize(num_layers);
        for (const auto & part : result->parts_with_ranges)
            if (auto it = layer_of_partition.find(part.data_part->info.getPartitionId()); it != layer_of_partition.end())
                result->split_parts.layers[it->second].push_back(part);
        reading.setAnalyzedResult(std::move(result));
    };
    split_by_layers(*left_reading, *left_analysis);
    split_by_layers(*right_reading, *right_analysis);

    std::ranges::sort(used_keys);
    used_keys.erase(std::ranges::unique(used_keys).begin(), used_keys.end());
    JoinStep::PrimaryKeySharding sharding;
    for (size_t key : used_keys)
        sharding.emplace_back(clause.key_names_left[key], clause.key_names_right[key]);
    join_step.enableJoinByLayers(std::move(sharding));
}

/// Execute a hash join of two MergeTree reads partition by partition.
///
/// If both tables are partitioned by the same function of the join keys (e.g. both are
/// `PARTITION BY toYYYYMM(date)` and joined `ON l.date = r.date`), a row of a partition of one table can
/// match only the rows of the partition with the same ID in the other table. The partitions both sides
/// read are split into groups, each read creates one output port per group, and the join is executed
/// port by port with a small independent hash table per group (`JoinStep::enableJoinByLayers` ->
/// `joinPipelinesByShards`). The rows are not scattered by the hash of the keys, and the partitions which
/// cannot produce output rows are not read.
///
/// Unlike `optimizeJoinByShards`, it applies to a single join whose inputs read MergeTree tables directly.
void optimizeJoinByPartitions(QueryPlan::Node & root)
{
    std::stack<QueryPlan::Node *> stack;
    stack.push(&root);

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        for (auto * child : node->children)
            stack.push(child);

        auto * join_step = typeid_cast<JoinStep *>(node->step.get());
        if (join_step && node->children.size() == 2 && !join_step->isJoinByLayersEnabled())
            tryJoinByPartitions(*node, *join_step);
    }
}

}
}
