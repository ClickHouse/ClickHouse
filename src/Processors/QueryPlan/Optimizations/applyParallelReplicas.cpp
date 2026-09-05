#include <memory>
#include <Core/Joins.h>
#include <Core/Settings.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>
#include <Processors/QueryPlan/ParallelReplicasSplitStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromParallelReplicas.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMerge.h>
#include <Common/logger_useful.h>

#include <unordered_set>

namespace DB
{
namespace Setting
{
extern const SettingsBool parallel_replicas_allow_merge_tables;
extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
}

namespace QueryPlanOptimizations
{

constexpr bool debug_logging_enabled = false;

/// Plan-wide collector of the MergeTree reads to distribute (defined below; used by buildPlanFragment).
/// A `MergeTree` read the pass would distribute, and the table it reads. A `Merge` read contributes one
/// entry per underlying table while `consider_merges` is set - all sharing that one node - which is what the
/// duplicate check of a union needs in order to judge a plan before any `Merge` is expanded.
struct ReadToDistribute
{
    QueryPlan::Node * node;
    StorageID storage_id;
};

static std::vector<ReadToDistribute> collectReadsToDistribute(QueryPlan::Node * node, bool consider_merges = false);

/// Side of a JOIN; `Left`/`Right` double as the join node's child indices.
enum class JoinSide : size_t
{
    Left = 0,
    Right = 1,
    None = 2,
};

/// Coordinated side of an eligible JOIN (the side split across replicas): left for INNER (ALL) and LEFT,
/// right for RIGHT; `None` otherwise (FULL/CROSS/COMMA/PASTE, INNER non-ALL). The other side is read in
/// full by every replica.
static JoinSide coordinatedJoinSide(const QueryPlan::Node * node)
{
    /// The pass runs before logical joins are converted to physical (see optimizeTreeSecondPass), so an
    /// eligible join is always a JoinStepLogical here.
    const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get());
    if (!join)
        return JoinSide::None;

    const JoinKind kind = join->getJoinOperator().kind;
    const JoinStrictness strictness = join->getJoinOperator().strictness;

    if ((kind == JoinKind::Inner && strictness == JoinStrictness::All) || kind == JoinKind::Left)
        return JoinSide::Left;
    if (kind == JoinKind::Right)
        return JoinSide::Right;

    return JoinSide::None;
}

/// Can this MergeTree read be part of a shipped fragment?
static bool mergeTreeReadCanBeShipped(const ReadFromMergeTree & read)
{
    /// A refreshable MaterializedView that swaps its target on each refresh (non-APPEND) must stay
    /// local: the target read is shipped by name and re-resolved per replica without RefreshTask's
    /// sync/lock, so a refresh could swap or drop it under the remote read. RefreshSet registers
    /// exactly these swap targets. An APPEND refreshable MV reads a fixed target (like a regular MV)
    /// and is safe to distribute.
    const auto & mergetree_data = read.getMergeTreeData();
    if (read.getContext()->getRefreshSet().tryGetTaskForInnerTable(mergetree_data.getStorageID()))
        return false;

    /// Top-K filtering puts an internal `__topKFilter` function into the read's PREWHERE and shares a runtime
    /// `TopKThresholdTracker` with the sorting step. The function is created on demand and is not registered
    /// in `FunctionFactory`, so a replica deserializing the fragment fails with `Unknown function
    /// __topKFilter`. Keeping such a read local is what `tryOptimizeTopK` does for `make_distributed_plan`,
    /// except that the decision has to be made here: enabling plan-based parallel replicas does not by itself
    /// mean the plan gets shipped, so suppressing Top-K up front would lose it for queries that stay local.
    if (read.isSelectedForTopKFilterOptimization())
        return false;

    /// Direct read from a text index (`query_plan_direct_read_from_text_index`) rewrites the text-search
    /// functions of the read's PREWHERE into `__text_index_*` virtual columns, which only this read's
    /// index read tasks materialize. `ReadFromMergeTree::serialize` does not carry that task map, so a
    /// replica deserializing the fragment fails with `Column '__text_index_...' not found in table`
    /// while building the read step. Shipping it also breaks execution when PREWHERE and WHERE carry
    /// different text-search queries, see https://github.com/ClickHouse/ClickHouse/issues/113664.
    /// `supportsBucketedRead` refuses a bucketed distributed read for the same reason, but nothing
    /// rejects an ordinary shipped fragment - `isSerializable` is unconditionally true for this step -
    /// so the read has to be kept local here, as for Top-K above.
    if (!read.getIndexReadTasks().empty())
        return false;

    /// The pinned block-number boundary is not serialized: a follower rebuilds the read with
    /// max_block_numbers_to_read = nullptr and would read past the initiator's snapshot boundary.
    if (read.hasPinnedBlockNumbers())
        return false;

    /// A non-replicated table can hold different data on each replica, so reading it remotely is opt-in.
    return mergetree_data.supportsReplication()
        || read.getContext()->getSettingsRef()[Setting::parallel_replicas_for_non_replicated_merge_tree];
}

/// The broadcast side of a shipped join is executed in full by every replica, so its MergeTree reads must
/// pass the same rules as the coordinated ones - otherwise each replica would join against its own data.
static bool subtreeHasUnshippableRead(const QueryPlan::Node * node)
{
    if (!node)
        return false;
    if (const auto * read = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
        return !mergeTreeReadCanBeShipped(*read);
    for (const auto * child : node->children)
        if (subtreeHasUnshippableRead(child))
            return true;
    return false;
}

/// A fragment is cloned and then serialized, so every step in it must be serializable. Checking that
/// generically (instead of enumerating step types) keeps new non-serializable steps out automatically:
/// a prepared-lookup join (JoinStepLogicalLookup) and correlated-subquery decorrelation (which buffers a
/// subplan through an in-process ChunkBuffer) are both rejected this way. Split markers are exempt: they
/// are consumed when the fragment is built (see ConvertToDistributedVisitor) and never get serialized.
static bool subtreeIsShippable(const QueryPlan::Node * node)
{
    const auto ignore_split_marker
        = [](const IQueryPlanStep & step) { return typeid_cast<const ParallelReplicasSplitStep *>(&step) != nullptr; };

    const auto * offending = findNonSerializableStep(node, ignore_split_marker);
    if (!offending)
        return true;

    LOG_DEBUG(
        getLogger("ApplyParallelReplicas"),
        "Keeping the plan fragment local: step '{}' is not serializable for remote execution",
        offending->step->getName());
    return false;
}

class ApplyParallelReplicasVisitor : public QueryPlanVisitor<ApplyParallelReplicasVisitor, debug_logging_enabled>
{
    QueryPlan::Nodes & nodes;
    const QueryPlanOptimizationSettings & optimization_settings;

public:
    ApplyParallelReplicasVisitor(
        QueryPlan::Node * root_, QueryPlan::Nodes & nodes_, const QueryPlanOptimizationSettings & optimization_settings_)
        : QueryPlanVisitor<ApplyParallelReplicasVisitor, debug_logging_enabled>(root_)
        , nodes(nodes_)
        , optimization_settings(optimization_settings_)
    {
    }

    bool visitTopDownImpl(QueryPlan::Node *, QueryPlan::Node *)
    {
        // if (!parent_node)
        //     return true;
        //
        // // there is no need to visit nodes below split step
        // auto * split_step = typeid_cast<ParallelReplicasSplitStep *>(parent_node->step.get());
        // if (split_step)
        //     return false;
        //
        return true;
    }

    /// If `node` is a UnionStep whose every branch is a split marker, pull a single split above the
    /// union (the union then operates on the reads directly). `node` becomes that split, so the split
    /// keeps lifting through the code below (e.g. to a partial aggregation above the union) and the whole
    /// union ships as one fragment. Branches are left untouched if not all of them are split markers
    /// (e.g. a mixed view over MergeTree + non-MergeTree), so those distribute per branch instead.
    void liftSplitsAboveUnion(QueryPlan::Node * node)
    {
        if (!typeid_cast<UnionStep *>(node->step.get()) || node->children.empty())
            return;

        for (const auto * child : node->children)
            if (!typeid_cast<const ParallelReplicasSplitStep *>(child->step.get()))
                return;

        auto & union_node = nodes.emplace_back();
        union_node.step = std::move(node->step);
        union_node.children.reserve(node->children.size());
        for (auto * child : node->children)
            union_node.children.push_back(child->children.front());

        node->step = std::make_unique<ParallelReplicasSplitStep>(union_node.step->getOutputHeader());
        node->children = {&union_node};
    }

    /// If `node` is an eligible JOIN whose coordinated-side child is a split marker, pull the split above the
    /// join so the whole join ships as one fragment (coordinated side read directly, other side
    /// broadcast). `node` becomes the split and keeps lifting through the code below.
    void liftSplitAboveJoin(QueryPlan::Node * node)
    {
        const auto coordinated_side = coordinatedJoinSide(node);
        if (coordinated_side == JoinSide::None)
            return;

        auto * coordinated_child = node->children[static_cast<size_t>(coordinated_side)];
        if (!typeid_cast<const ParallelReplicasSplitStep *>(coordinated_child->step.get()))
            return;

        /// Do not lift a split into a fragment with a non-serializable step. Not lifting keeps the split
        /// below the join, so the coordinated read is still distributed - only the join stays local.
        if (!subtreeIsShippable(node))
            return;

        /// Only the broadcast side needs this: everything under a split marker was already validated when
        /// the marker was planted, or by a nested lift.
        const auto broadcast_side = coordinated_side == JoinSide::Left ? JoinSide::Right : JoinSide::Left;
        if (subtreeHasUnshippableRead(node->children[static_cast<size_t>(broadcast_side)]))
            return;

        auto & join_node = nodes.emplace_back();
        join_node.step = std::move(node->step);
        join_node.children = node->children;
        join_node.children[static_cast<size_t>(coordinated_side)] = coordinated_child->children.front();

        node->step = std::make_unique<ParallelReplicasSplitStep>(join_node.step->getOutputHeader());
        node->children = {&join_node};
    }

    void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        liftSplitsAboveUnion(current_node);
        liftSplitAboveJoin(current_node);

        if (!parent_node)
            return;

        auto * original_split_step = typeid_cast<ParallelReplicasSplitStep *>(current_node->step.get());
        if (!original_split_step)
            return;

        auto * original_split_node = current_node;
        const auto * parent_step = parent_node->step.get();

        /// BuildRuntimeFilterStep sits above the join's build side, which for a RIGHT join is the coordinated
        /// side, so the split step has to pass it too. The step becomes part of the plan fragment, where it
        /// does nothing: a deserialized step cannot publish its filter, so every replica builds its own.
        if (typeid_cast<const ExpressionStep *>(parent_step) || typeid_cast<const FilterStep *>(parent_step)
            || typeid_cast<const BuildRuntimeFilterStep *>(parent_step))
        {
            /// Move the split step above the expression/filter step and update its header to match
            /// the new child, since the split step just passes data through.
            std::swap(current_node->step, parent_node->step);
            parent_node->step->updateInputHeader(current_node->step->getOutputHeader());
            return;
        }

        const auto * aggregating_step = typeid_cast<const AggregatingStep *>(parent_step);
        if (aggregating_step)
        {
            /// Params will be used by merge step
            Aggregator::Params aggregator_params = aggregating_step->getParams();
            GroupingSetsParamsList grouping_sets_params = aggregating_step->getGroupingSetsParamsList();

            const bool should_produce_results_in_order_of_bucket_number = aggregating_step->shouldProduceResultsInBucketOrder();
            const bool memory_bound_merging_of_aggregation_results_enabled = aggregating_step->usingMemoryBoundMerging();
            const bool original_step_was_final
                = aggregating_step->getFinal(); /// Save whether the original AggregatingStep was final or partial

            /// Merging the results of the replicas is the same as merging the results of the shards of a
            /// `Distributed` table, so it obeys the same setting. Note that this is not only about the memory:
            /// the ordinary merging transform returns the two-level buckets in an arbitrary order, which the
            /// node above cannot merge memory efficiently.
            /// Grouping sets are not supported by the memory efficient merging, see `MergingAggregatedStep`.
            const bool memory_efficient_aggregation = optimization_settings.distributed_aggregation_memory_efficient
                && grouping_sets_params.empty() && !aggregating_step->getOutputHeader()->has("__grouping_set");

            /// The memory-efficient merge consumes each input as a stream of buckets in ascending
            /// order, so the partial aggregation must produce its result in bucket order.
            auto & partial_aggregation_node = nodes.emplace_back();
            partial_aggregation_node.step = aggregating_step->clone();
            auto * partial_aggregation_step = typeid_cast<AggregatingStep *>(partial_aggregation_node.step.get());
            partial_aggregation_step->setFinal(false);
            /// Keep the bucket order when the original step already promised it to its consumer.
            partial_aggregation_step->setProduceResultsInBucketOrder(
                should_produce_results_in_order_of_bucket_number || memory_efficient_aggregation);
            partial_aggregation_node.step->setStepDescription("partial");
            partial_aggregation_node.children = {original_split_node->children.front()};

            /// Add gather
            auto & new_split_node = nodes.emplace_back();
            new_split_node.step = std::make_unique<ParallelReplicasSplitStep>(partial_aggregation_node.step->getOutputHeader());
            new_split_node.children = {&partial_aggregation_node};

            /// Replace original aggregation step with MergingAggregated step
            aggregator_params.only_merge = true; /// Merge partial aggregation results
            QueryPlanStepPtr final_aggregation_step = std::make_unique<MergingAggregatedStep>(
                new_split_node.step->getOutputHeader(),
                aggregator_params,
                grouping_sets_params,
                /* final */ original_step_was_final,
                memory_efficient_aggregation,
                aggregating_step->getTemporaryDataMergeThreads(),
                should_produce_results_in_order_of_bucket_number,
                aggregating_step->getMaxBlockSize(),
                aggregating_step->getMaxBlockSizeForAggregationInOrder(),
                memory_bound_merging_of_aggregation_results_enabled);

            final_aggregation_step->setStepDescription("merge");
            parent_node->step = std::move(final_aggregation_step);
            parent_node->children = {&new_split_node};
            return;
        }

        /// Ship the sort with the fragment and merge the already sorted streams on the initiator
        const auto * sorting_step = typeid_cast<const SortingStep *>(parent_step);
        if (sorting_step && sortingCanBeShipped(*sorting_step) && subtreeIsShippable(parent_node))
        {
            const auto sort_description = sorting_step->getSortDescription();
            const UInt64 limit = sorting_step->getLimit();
            /// With `exact_rows_before_limit` the bound must not be shipped: a `LimitStep` in the fragment
            /// would truncate a replica's stream before `rows_before_limit_at_least` is counted.
            /// FIXME(#114723): the count is still inexact (as in classic parallel replicas) because the cloned
            /// sort keeps its limit on the local half; rebuilding it unbounded fixes the count but hangs the
            /// merge, which reaches its own limit and then never completes its `always_read_till_end` drain.
            const bool read_till_end = mustReadTillEnd();

            /// Per-replica sort. Still a full sort here: read-in-order runs later, separately on each side.
            auto & partial_sorting_node = nodes.emplace_back();
            partial_sorting_node.step = sorting_step->clone();
            partial_sorting_node.step->setStepDescription("partial");
            partial_sorting_node.children = {original_split_node->children.front()};

            QueryPlan::Node * fragment_root = &partial_sorting_node;

            /// `SortingStep::serialize` drops the limit and `deserialize` rebuilds an unbounded sort, so a
            /// top-N has to be restated as a step to survive the wire. The offset is deliberately not shipped:
            /// it applies once, globally, above the merge.
            if (const UInt64 local_limit = read_till_end ? 0 : limit)
            {
                auto & limit_node = nodes.emplace_back();
                limit_node.step = std::make_unique<LimitStep>(partial_sorting_node.step->getOutputHeader(), local_limit, 0);
                limit_node.step->setStepDescription("local top-N");
                limit_node.children = {&partial_sorting_node};
                fragment_root = &limit_node;
            }

            auto & new_split_node = nodes.emplace_back();
            new_split_node.step = std::make_unique<ParallelReplicasSplitStep>(fragment_root->step->getOutputHeader());
            new_split_node.children = {fragment_root};

            /// Each replica returns one sorted stream, so the initiator only has to merge them.
            auto merging_sorted_step = std::make_unique<SortingStep>(
                new_split_node.step->getOutputHeader(), sort_description, sorting_step->getSettings(), limit, read_till_end);
            merging_sorted_step->setStepDescription("merge sorted streams from replicas");
            parent_node->step = std::move(merging_sorted_step);
            parent_node->children = {&new_split_node};
            return;
        }
    }

private:
    /// True if any ancestor LIMIT must read till the end (`exact_rows_before_limit`).
    bool mustReadTillEnd() const
    {
        for (const auto & frame : stack)
            if (const auto * limit = typeid_cast<const LimitStep *>(frame.node->step.get()))
                if (limit->alwaysReadTillEnd())
                    return true;
        return false;
    }

    /// The sort is cloned into the fragment and serialized, so it must be serializable - which for a
    /// `SortingStep` means a plain full sort. That holds for an ordinary ORDER BY here, because this pass runs
    /// before `optimizeReadInOrder` and `applyOrder` convert sorts to `FinishSorting`. A sort feeding a full
    /// sorting merge join is excluded separately: its output is consumed by a join on the initiator rather
    /// than merged, so replacing it with a merge of per-replica sorts would change what the join sees.
    ///
    /// A partitioned sort is a window pre-sort, and it is serializable, but its contract is one stream per
    /// PARTITION BY group rather than one sorted stream: it scatters by the partition keys and skips the final
    /// merge, so `WindowStep` above it runs one `WindowTransform` per stream. The `MergingSorted` step put on
    /// the initiator cannot express that, so shipping such a sort collapses both the sort and the window to a
    /// single stream. Keeping the split below it loses nothing - read-in-order does not apply to a partitioned
    /// sort unless `query_plan_reuse_storage_ordering_for_window_functions` is enabled - and matches classic
    /// parallel replicas, which computes windows on the initiator. See
    /// https://github.com/ClickHouse/ClickHouse/issues/115174
    static bool sortingCanBeShipped(const SortingStep & sorting_step)
    {
        return sorting_step.isSerializable() && !sorting_step.isSortingForMergeJoin() && !sorting_step.hasPartitions();
    }
};

class ConvertToDistributedVisitor : public QueryPlanVisitor<ConvertToDistributedVisitor, debug_logging_enabled>
{
    QueryPlan & query_plan;

public:
    explicit ConvertToDistributedVisitor(QueryPlan & query_plan_)
        : QueryPlanVisitor<ConvertToDistributedVisitor, debug_logging_enabled>(query_plan_.getRootNode())
        , query_plan(query_plan_)
    {
    }

    bool visitTopDownImpl(QueryPlan::Node *, QueryPlan::Node *)
    {
        // if (!parent_node)
        //     return true;
        //
        // // there is no need to visit nodes below split step
        // auto * split_step = typeid_cast<ParallelReplicasSplitStep *>(parent_node->step.get());
        // if (split_step)
        //     return false;

        return true;
    }

    void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node *)
    {
        auto * split_step = typeid_cast<ParallelReplicasSplitStep *>(current_node->step.get());
        if (!split_step)
            return;

        // build plan fragment
        auto [plan_fragment, context] = buildPlanFragment(current_node);

        auto parallel_replicas_plan = ClusterProxy::createParallelReplicasPlan(std::move(plan_fragment), context);
        if (!parallel_replicas_plan)
            return;

        query_plan.replaceNodeWithPlan(current_node, std::move(*parallel_replicas_plan));
    }

private:
    std::pair<QueryPlanPtr, ContextPtr> buildPlanFragment(QueryPlan::Node * split_node)
    {
        /// The split marker is a unary pass-through; the fragment to distribute is the subtree below
        /// it. Clone it structurally: `QueryPlan::addStep` can only replay a linear chain and would
        /// throw on a branching fragment (e.g. a view expanding to UNION ALL, or a JOIN).
        auto plan_fragment = std::make_unique<QueryPlan>(QueryPlan::cloneSubtree(split_node->children.front(), query_plan));

        ContextPtr context;
        /// Mark only the coordinated reads (collectReadsToDistribute follows a join's coordinated side) so they
        /// are deserialized in parallel-reading mode; the other side stays unmarked and is broadcast.
        for (const auto & read : collectReadsToDistribute(plan_fragment->getRootNode()))
        {
            auto * read_step = typeid_cast<ReadFromMergeTree *>(read.node->step.get());
            read_step->enableParallelReadingFromReplicasForSerialization();
            context = read_step->getContext();
        }

        return {std::move(plan_fragment), context};
    }
};


/// Plan-wide collector of the MergeTree reads to distribute. Unlike findReadingSteps (the
/// view-expansion helper), this descends into every UnionStep -- including one at the plan root (a
/// top-level UNION ALL) -- so all of its branches ship as a single fragment. For a JOIN it follows the
/// single parallelized side. A union whose branches read the same table more than once is rejected (its
/// reads are left local): the parallel-replicas coordinator drives every read of a shipped fragment and
/// cannot distinguish duplicate announcements for one table, so such a union must not become a single
/// distributed fragment (mirrors StorageView::getUnderlyingMergeTreeStorageForParallelReplicas).
static std::vector<ReadToDistribute> collectReadsToDistribute(QueryPlan::Node * node, bool consider_merges)
{
    if (!node)
        return {};

    if (auto * read = typeid_cast<ReadFromMergeTree *>(node->step.get()))
    {
        if (!mergeTreeReadCanBeShipped(*read))
            return {};
        return {{node, read->getMergeTreeData().getStorageID()}};
    }

    /// A `Merge` read is still opaque at this point, so answer for the union it would be expanded into: the
    /// reads of its underlying tables, all attributed to this node. That makes the verdict below - including
    /// the duplicate check of an enclosing union - the same one the expanded plan would get, so the plan is
    /// rewritten only when the rewrite is of use.
    if (consider_merges)
    {
        if (auto * merge = typeid_cast<ReadFromMerge *>(node->step.get()))
        {
            if (!merge->getContext()->getSettingsRef()[Setting::parallel_replicas_allow_merge_tables])
                return {};

            const auto & storage_ids = merge->getExpandableReads(mergeTreeReadCanBeShipped);

            std::vector<ReadToDistribute> reads;
            reads.reserve(storage_ids.size());
            for (const auto & storage_id : storage_ids)
                reads.push_back({node, storage_id});
            return reads;
        }
    }

    if (typeid_cast<UnionStep *>(node->step.get()))
    {
        std::vector<ReadToDistribute> reads;
        for (auto * child : node->children)
        {
            auto child_reads = collectReadsToDistribute(child, consider_merges);
            reads.insert(reads.end(), child_reads.begin(), child_reads.end());
        }

        std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> seen;
        for (const auto & read : reads)
            if (!seen.insert(read.storage_id).second)
                return {};

        return reads;
    }

    if (node->children.empty())
        return {};

    if (typeid_cast<const JoinStepLogical *>(node->step.get()))
    {
        /// Distribute only the join kinds where splitting one side across replicas and concatenating the
        /// per-replica results yields the correct join (see coordinatedJoinSide): INNER (ALL) and
        /// LEFT coordinate the left side, RIGHT coordinates the right side. FULL/CROSS/COMMA/PASTE are kept local.
        /// Whether the join itself can ship is decided later, by liftSplitAboveJoin: this runs before any
        /// split marker exists, and rejecting the join here would leave the coordinated read unmarked, so
        /// nothing at all would be distributed.
        const auto coordinated_side = coordinatedJoinSide(node);
        if (coordinated_side == JoinSide::None)
            return {};

        return collectReadsToDistribute(node->children.at(static_cast<size_t>(coordinated_side)), consider_merges);
    }

    /// Non-join single-input step (Expression/Filter/Sorting/...): follow the only input.
    return collectReadsToDistribute(node->children.at(0), consider_merges);
}

/// FINAL is incompatible with parallel-replica reading (the FINAL merge path requires the read not to be
/// in parallel-reading mode). Classic parallel replicas disables PR for the whole query when FINAL is
/// present; do the same here, so a plan with any FINAL MergeTree read is executed locally.
static bool planHasFinalMergeTreeRead(const QueryPlan::Node * node)
{
    if (!node)
        return false;
    if (const auto * read = typeid_cast<const ReadFromMergeTree *>(node->step.get()); read && read->isQueryWithFinal())
        return true;
    for (const auto * child : node->children)
        if (planHasFinalMergeTreeRead(child))
            return true;
    return false;
}

/// A `FutureSetFromSubquery` (e.g. `WHERE x IN (SELECT ...)`) cannot yet be shipped: `addStepsToBuildSets`
/// moves the subquery's plan out before the captured fragment is serialized, so serialization throws a
/// `LOGICAL_ERROR` (#111876). Until fixed, detect the still-intact `DelayedCreatingSetsStep` and run the
/// query locally, like the FINAL case above.
/// TODO(#111876): serialize the subquery set at fragment-capture time so `IN (subquery)` can be distributed.
static bool planHasSubquerySet(const QueryPlan::Node * node)
{
    if (!node)
        return false;
    if (const auto * delayed = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()); delayed && !delayed->getSets().empty())
        return true;
    for (const auto * child : node->children)
        if (planHasSubquerySet(child))
            return true;
    return false;
}

/// A `Merge` table is opaque to the collectors above: `ReadFromMerge` unites the pipelines of its
/// per-table subplans instead of their plans, so the underlying `MergeTree` reads do not exist yet while
/// the plan is transformed. Expand every eligible `ReadFromMerge` into a plan-level union of those reads
/// first, so that the rest of the pass treats a `Merge` exactly like a `UNION ALL` over its underlying
/// tables. Ineligible ones (a child which is not a plain `MergeTree` read, a `FINAL` read, nothing to read)
/// are left as they are and read by a single replica. Call it only once the plan is known to distribute
/// something - see the caller.
static void expandMergeReadsForParallelReplicas(QueryPlan & query_plan)
{
    auto * root = query_plan.getRootNode();
    if (!root)
        return;

    /// Collect first: the expansion replaces the step of a visited node.
    std::vector<QueryPlan::Node *> merge_nodes;
    Stack stack;
    traverseQueryPlan(
        stack,
        *root,
        [&](QueryPlan::Node & node)
        {
            const auto * merge = typeid_cast<const ReadFromMerge *>(node.step.get());
            if (merge && merge->getContext()->getSettingsRef()[Setting::parallel_replicas_allow_merge_tables])
                merge_nodes.push_back(&node);
        });

    for (auto * node : merge_nodes)
    {
        auto & merge = typeid_cast<ReadFromMerge &>(*node->step);
        if (!merge.getExpandableReads(mergeTreeReadCanBeShipped).empty())
            query_plan.replaceNodeWithPlan(node, merge.expandForParallelReplicas());
    }
}

/// Insertion phase: put a ParallelReplicasSplitStep directly above every eligible MergeTree read.
/// Raising the markers up the plan (through expressions, aggregation and unions) and rewriting them
/// into a distributed read is done by the phases below. The planner now builds only a plain local plan.
static void insertParallelReplicasSplit(QueryPlan & query_plan, QueryPlan::Nodes & nodes)
{
    auto * root = query_plan.getRootNode();
    if (!root)
        return;

    /// TODO: distribute the non-FINAL reads and keep only the FINAL ones local.
    /// Union with a mix of local and distributed branches currently is not supported,
    /// it can produce wrong results
    if (planHasFinalMergeTreeRead(root))
        return;

    if (planHasSubquerySet(root))
        return;

    /// Ask first whether anything would be distributed once the `Merge` reads are expanded into unions of
    /// the reads of their underlying tables. The answer is not a property of one read: a `FULL`/`CROSS` join
    /// yields nothing, and a union is rejected outright when two of its branches read the same table - which
    /// the expansion itself can cause, by turning a `Merge` into a union of the very tables a sibling branch
    /// reads. Deciding up front is what keeps a query which is not distributed on the plan it would have
    /// without the feature, instead of on a union nothing distributes.
    if (collectReadsToDistribute(root, /*consider_merges=*/ true).empty())
        return;

    /// Now the same union and aggregation splitting as for a plain `MergeTree` table applies to a `Merge`.
    /// Every eligible one is expanded, including a `Merge` on the broadcast side of a join, which has no read
    /// of its own to distribute but is shipped inside the fragment and read in full by every replica.
    expandMergeReadsForParallelReplicas(query_plan);

    std::unordered_set<const QueryPlan::Node *> eligible;
    for (const auto & read : collectReadsToDistribute(root))
        eligible.insert(read.node);
    if (eligible.empty())
        return;

    /// The split step is created directly above the read. When converting the split marker into a
    /// distributed fragment, the fragment's execution context is taken from the ReadFromMergeTree step.
    auto make_split_above = [&](QueryPlan::Node * read_node) -> QueryPlan::Node *
    {
        auto & split_node = nodes.emplace_back();
        split_node.step = std::make_unique<ParallelReplicasSplitStep>(read_node->step->getOutputHeader());
        split_node.children = {read_node};
        return &split_node;
    };

    if (eligible.contains(root))
    {
        query_plan.replaceRootNode(make_split_above(root));
        return;
    }

    /// Collect (parent, child index) of every eligible read first, then wrap — avoids mutating the tree
    /// while traversing it.
    std::vector<std::pair<QueryPlan::Node *, size_t>> to_wrap;
    Stack stack;
    traverseQueryPlan(
        stack,
        *root,
        [&](QueryPlan::Node & node)
        {
            for (size_t i = 0; i < node.children.size(); ++i)
                if (eligible.contains(node.children[i]))
                    to_wrap.emplace_back(&node, i);
        });

    for (auto & [parent, i] : to_wrap)
        parent->children[i] = make_split_above(parent->children[i]);
}

void applyParallelReplicas(QueryPlan & query_plan, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings &);

void applyParallelReplicas(QueryPlan & query_plan, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    if (!settings.enable_parallel_replicas)
        return;

    insertParallelReplicasSplit(query_plan, nodes);

    ApplyParallelReplicasVisitor(query_plan.getRootNode(), nodes, settings).visit();

    ConvertToDistributedVisitor(query_plan).visit();
}

}

}
