#include <memory>
#include <optional>
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
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/logger_useful.h>

#include <unordered_set>

namespace DB
{
namespace Setting
{
extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
}

namespace QueryPlanOptimizations
{

constexpr bool debug_logging_enabled = false;

/// Plan-wide collector of the MergeTree reads to distribute (defined below; used by buildPlanFragment).
static std::vector<QueryPlan::Node *> collectReadsToDistribute(QueryPlan::Node * node);

/// Coordinated-side child index for an eligible JOIN (the side split across replicas): 0 for INNER (ALL)
/// and LEFT, 1 for RIGHT; nullopt otherwise (FULL/CROSS/COMMA/PASTE, INNER non-ALL). The other side is
/// read in full by every replica.
static std::optional<size_t> coordinatedJoinSideIndex(const QueryPlan::Node * node)
{
    /// The pass runs before logical joins are converted to physical (see optimizeTreeSecondPass), so an
    /// eligible join is always a JoinStepLogical here.
    const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get());
    if (!join)
        return {};

    const JoinKind kind = join->getJoinOperator().kind;
    const JoinStrictness strictness = join->getJoinOperator().strictness;

    if ((kind == JoinKind::Inner && strictness == JoinStrictness::All) || kind == JoinKind::Left)
        return 0;
    if (kind == JoinKind::Right)
        return 1;
    return {};
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
        const auto coordinated_index = coordinatedJoinSideIndex(node);
        if (!coordinated_index)
            return;

        auto * coordinated_child = node->children[*coordinated_index];
        if (!typeid_cast<const ParallelReplicasSplitStep *>(coordinated_child->step.get()))
            return;

        /// Do not lift a split into a fragment that would contain a non-serializable step, or a MergeTree
        /// read which must not be executed on every replica (the broadcast side is never checked by
        /// collectReadsToDistribute, which only follows the coordinated side). This is the only place where
        /// a join is rejected: not lifting keeps the coordinated read's split below the join, so that read
        /// is still distributed and only the join itself stays local.
        /// These walk the whole subtree, so they run last: a join with nothing to lift never pays for them.
        if (!subtreeIsShippable(node) || subtreeHasUnshippableRead(node))
            return;

        auto & join_node = nodes.emplace_back();
        join_node.step = std::move(node->step);
        join_node.children = node->children;
        join_node.children[*coordinated_index] = coordinated_child->children.front();

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

            /// Convert Aggregation step to partial aggregation
            auto & partial_aggregation_node = nodes.emplace_back();
            partial_aggregation_node.step = aggregating_step->clone();
            typeid_cast<AggregatingStep *>(partial_aggregation_node.step.get())->setFinal(false);
            partial_aggregation_node.step->setStepDescription("partial");
            partial_aggregation_node.children = {original_split_node->children.front()};

            /// Add gather
            auto & new_split_node = nodes.emplace_back();
            new_split_node.step = std::make_unique<ParallelReplicasSplitStep>(partial_aggregation_node.step->getOutputHeader());
            new_split_node.children = {&partial_aggregation_node};

            /// Replace original aggregation step with MergingAggregated step
            aggregator_params.only_merge = true; /// Merge partial aggregation results
            /// Merging the results of the replicas is the same as merging the results of the shards of a
            /// `Distributed` table, so it obeys the same setting. Note that this is not only about the memory:
            /// the ordinary merging transform returns the two-level buckets in an arbitrary order, which the
            /// node above cannot merge memory efficiently.
            /// Grouping sets are not supported by the memory efficient merging, see `MergingAggregatedStep`.
            const bool memory_efficient_aggregation = optimization_settings.distributed_aggregation_memory_efficient
                && grouping_sets_params.empty() && !new_split_node.step->getOutputHeader()->has("__grouping_set");
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
        auto plan_fragment = std::make_unique<QueryPlan>(QueryPlan::cloneSubtree(split_node->children.front()));

        ContextPtr context;
        /// Mark only the coordinated reads (collectReadsToDistribute follows a join's coordinated side) so they
        /// are deserialized in parallel-reading mode; the other side stays unmarked and is broadcast.
        for (auto * read_node : collectReadsToDistribute(plan_fragment->getRootNode()))
        {
            auto * read_step = typeid_cast<ReadFromMergeTree *>(read_node->step.get());
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
static std::vector<QueryPlan::Node *> collectReadsToDistribute(QueryPlan::Node * node)
{
    if (!node)
        return {};

    if (auto * read = typeid_cast<ReadFromMergeTree *>(node->step.get()))
    {
        if (!mergeTreeReadCanBeShipped(*read))
            return {};
        return {node};
    }

    if (typeid_cast<UnionStep *>(node->step.get()))
    {
        std::vector<QueryPlan::Node *> reads;
        for (auto * child : node->children)
        {
            auto child_reads = collectReadsToDistribute(child);
            reads.insert(reads.end(), child_reads.begin(), child_reads.end());
        }

        std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> seen;
        for (auto * read_node : reads)
        {
            const auto & storage_id = typeid_cast<ReadFromMergeTree &>(*read_node->step).getMergeTreeData().getStorageID();
            if (!seen.insert(storage_id).second)
                return {};
        }
        return reads;
    }

    if (node->children.empty())
        return {};

    if (typeid_cast<const JoinStepLogical *>(node->step.get()))
    {
        /// Distribute only the join kinds where splitting one side across replicas and concatenating the
        /// per-replica results yields the correct join (see coordinatedJoinSideIndex): INNER (ALL) and
        /// LEFT coordinate the left side, RIGHT coordinates the right side. FULL/CROSS/COMMA/PASTE are kept local.
        /// Whether the join itself can ship is decided later, by liftSplitAboveJoin: this runs before any
        /// split marker exists, and rejecting the join here would leave the coordinated read unmarked, so
        /// nothing at all would be distributed.
        const auto coordinated_index = coordinatedJoinSideIndex(node);
        if (!coordinated_index)
            return {};

        return collectReadsToDistribute(node->children.at(*coordinated_index));
    }

    /// Non-join single-input step (Expression/Filter/Sorting/...): follow the only input.
    return collectReadsToDistribute(node->children.at(0));
}

/// FINAL is incompatible with parallel-replica reading (the FINAL merge path requires the read not to be
/// in parallel-reading mode). Classic parallel replicas disables PR for the whole query when FINAL is
/// present; do the same here, so a plan with any FINAL MergeTree read is executed locally.
/// TODO: distribute the non-FINAL reads and keep only the FINAL ones local.
/// Union with a mix of local and distributed branches currently is not supported,
/// it can produce wrong results
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
    if (const auto * delayed = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get());
        delayed && !delayed->getSets().empty())
        return true;
    for (const auto * child : node->children)
        if (planHasSubquerySet(child))
            return true;
    return false;
}

/// Insertion phase: put a ParallelReplicasSplitStep directly above every eligible MergeTree read.
/// Raising the markers up the plan (through expressions, aggregation and unions) and rewriting them
/// into a distributed read is done by the phases below. The planner now builds only a plain local plan.
static void insertParallelReplicasSplit(QueryPlan & query_plan, QueryPlan::Nodes & nodes)
{
    auto * root = query_plan.getRootNode();
    if (!root)
        return;

    if (planHasFinalMergeTreeRead(root))
        return;

    if (planHasSubquerySet(root))
        return;

    std::unordered_set<const QueryPlan::Node *> eligible;
    for (auto * node : collectReadsToDistribute(root))
        eligible.insert(node);
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
