#include <Processors/Sources/RecursiveCTESource.h>

#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageView.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Set.h>

#include <QueryPipeline/SizeLimits.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Planner/findQueryForParallelReplicas.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeTuple.h>

#include <Common/Arena.h>
#include <Common/assert_cast.h>

#include <map>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_recursive_cte_evaluation_depth;
    extern const SettingsUInt64 recursive_cte_max_in_filter_cardinality;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsBool transform_null_in;
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool validate_enum_literals_in_operators;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsUInt64 parallel_replicas_min_number_of_rows_per_replica;
    extern const SettingsBool parallel_replicas_plan_based;
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
    extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
    extern const SettingsBool optimize_skip_unused_shards;
    extern const SettingsBool allow_nondeterministic_optimize_skip_unused_shards;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

std::vector<TableNode *> collectTableNodesWithTemporaryTableName(const std::string & temporary_table_name, IQueryTreeNode * root)
{
    std::vector<TableNode *> result;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto * table_node = subtree_node->as<TableNode>();
        if (table_node && table_node->getTemporaryTableName() == temporary_table_name)
            result.push_back(table_node);

        for (auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return result;
}

/// How a query subtree could engage parallel replicas. The two flavors matter because the
/// planner treats them differently under the forcing mode: for a read it serves from a local
/// `MergeTree`-family table it may still *silently disable* parallel replicas after estimating
/// the number of rows to read (`parallel_replicas_min_number_of_rows_per_replica`, see
/// `PlannerJoinTree`), even with `allow_experimental_parallel_reading_from_replicas = 2`,
/// while a read shipped through `ClusterProxy` never runs that estimate.
struct ParallelReplicasEngagement
{
    /// Via the planner's own storage-level rule (`canUseTableForParallelReplicas`): a local
    /// `MergeTree`-family table (possibly behind a view), subject to the row-count estimate.
    bool local_merge_tree = false;
    /// Number of local `MergeTree` reads that can occur in this plan fragment.
    /// The planner applies the min-rows estimate only when its candidate plan has
    /// exactly one `ReadFromMergeTree` step.
    size_t local_merge_tree_read_count = 0;
    /// A `Merge` read can prune its children by `_table` / `_database` before
    /// planning. The preflight observes the unpruned child set, so its count can
    /// be higher than the count the planner uses for the min-rows estimate.
    bool local_merge_tree_read_count_may_be_reduced_by_merge_pruning = false;
    /// Via a `ClusterProxy`-served storage (`Distributed` and its wrappers): no row-count
    /// estimate applies, the parallel-replica settings are honoured as-is.
    bool remote = false;

    bool any() const { return local_merge_tree || remote; }

    void merge(const ParallelReplicasEngagement & other)
    {
        local_merge_tree |= other.local_merge_tree;
        local_merge_tree_read_count += other.local_merge_tree_read_count;
        local_merge_tree_read_count_may_be_reduced_by_merge_pruning
            |= other.local_merge_tree_read_count_may_be_reduced_by_merge_pruning;
        remote |= other.remote;
    }
};

/// Whether parallel replicas could actually be engaged for a remote storage.
///
/// A remote read does not use parallel replicas just because the settings ask for it: the
/// cluster also has to have a shape the algorithms can split. `ClusterProxy::
/// updateSettingsAndClientInfoForCluster` turns task-based parallel replicas off for a
/// cluster whose every shard has a single node, and for a `remote()` table function without
/// a named cluster; `Context::canUseParallelReplicasCustomKeyForCluster` requires a single
/// shard with more than one node. Such reads therefore run as plain remote reads today, and
/// the forcing mode has nothing to fail on — the rejection must not fire for them either.
///
/// This is a positive capability check: among remote storages, only reads served by
/// `ClusterProxy::executeQuery` consult the parallel-replica settings at all, and the only
/// storage routing there is `StorageDistributed` (which backs `Distributed` tables and the
/// `remote` / `cluster` / `clusterAllReplicas` table functions). Every other remote engine —
/// `MongoDB`, `MySQL`, `PostgreSQL`, `YTsaurus`, the `*Cluster` object-storage functions, ... —
/// builds its source pipe directly and never looks at
/// `allow_experimental_parallel_reading_from_replicas` (the object-storage cluster conversion
/// driven by `parallel_replicas_for_cluster_engines` happens at table-function resolution
/// time, before this source runs, so by now it is an ordinary `IStorageCluster` read), so
/// disabling the setting cannot downgrade them and the forcing mode has nothing to fail on.
/// Storages that delegate their read to another storage are unwrapped: a table defined
/// `AS remote(...)` is a `StorageProxy` over `StorageDistributed`, a materialized view
/// with a `Distributed` target reads that target directly (this is independent of
/// `parallel_replicas_allow_materialized_views`, which gates only the planner's
/// `MergeTree`-family rule), an `Alias` table reads its target, a `Buffer` table
/// forwards both `getQueryProcessingStage` and `read` to its destination, and a `Merge`
/// table plans each child with the same query context (`ReadFromMerge` calls the child's
/// `read` directly, so a remote child's read still goes through `ClusterProxy` and
/// consults the parallel-replica settings). `Merge` prunes the child set per query
/// (`ReadFromMerge::getSelectedTables` evaluates `_table` / `_database` filters), but
/// only for children that do not read their data from other tables
/// (`IStorage::readsFromOtherTables`) — a `Distributed` / `Alias` / `Buffer` / nested
/// `Merge` child stamps its rows with the name of the table that actually produced them
/// and is always read, so its engagement is unconditional. The prunable children count
/// only when every one of them is eligible: which of them survive pruning depends on the
/// query and is not known before planning, so a prunable set with an ineligible member
/// contributes nothing — for a query that does read an eligible prunable remote child
/// this under-throws, the same documented trade-off as above: the read stays correct,
/// parallel replicas are just silently kept off for the recursive step. When every
/// prunable child is eligible, any non-empty pruned subset still is, so failing closed
/// remains correct (a filter matching no prunable child makes their part of the step an
/// empty plain read; over-throwing on that degenerate case is accepted), except that a
/// prunable set mixing engagement *flavors* keeps only the estimate-subject one, since
/// pruning may leave the local `MergeTree` child alone (see the `Merge` branch below).
/// A `Merge` whose children are all local cannot
/// engage parallel replicas under the analyzer — the storage-level `MergeTree`
/// parallel-replica paths are old-analyzer-only, and the planner's rule rejects `Merge`
/// itself — so it counts as not eligible here.
/// A wrapper target that is an ordinary `VIEW` is not remote either, but its read
/// re-interprets the inner query with the reading context's settings and can engage
/// parallel replicas all the same, so unwrapped storages are judged with
/// `mayEngageParallelReplicasForWrappedStorage`, which sees through views.
ParallelReplicasEngagement mayEngageParallelReplicasForWrappedStorage(const StoragePtr & storage, const ContextPtr & context);

ParallelReplicasEngagement mayEngageParallelReplicasForRemoteStorage(const IStorage & storage, const ContextPtr & context)
{
    /// `isRemote` on these wrappers already forced the nested/target storage, so unwrapping
    /// it here has no extra side effect.
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(&storage))
        return mayEngageParallelReplicasForWrappedStorage(proxy->getNested(), context);

    if (const auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(&storage))
        return mayEngageParallelReplicasForWrappedStorage(materialized_view->tryGetTargetTable(), context);

    if (const auto * alias = dynamic_cast<const StorageAlias *>(&storage))
        return mayEngageParallelReplicasForWrappedStorage(alias->getTargetTable(), context);

    if (const auto * buffer = dynamic_cast<const StorageBuffer *>(&storage))
        return mayEngageParallelReplicasForWrappedStorage(buffer->getDestinationTable(), context);

    if (const auto * merge = dynamic_cast<const StorageMerge *>(&storage))
    {
        /// `ReadFromMerge::getSelectedTables` prunes the child set with `_table` /
        /// `_database` filters before creating child plans, but only the children that do
        /// not read their data from other tables (`IStorage::readsFromOtherTables`): the
        /// rows of a `Distributed`, `Alias`, `Buffer` or nested `Merge` child carry the
        /// name of the table that actually produced them, so such a child is always read
        /// and the predicate filters its rows. The engagement of a non-prunable child is
        /// therefore unconditional — even an ineligible sibling cannot remove its read.
        /// A prunable child's engagement may vanish with the child, so the prunable subset
        /// is judged as a whole: eligible only when every prunable child is — any subset
        /// of an all-eligible set is still eligible, while which children survive pruning
        /// is not known before planning, so a set with an ineligible prunable child
        /// contributes nothing. For a query that does read an eligible prunable child this
        /// under-throws — the documented trade-off: the read stays correct, parallel
        /// replicas are just silently kept off for the recursive step.
        bool has_children = false;
        bool has_prunable_child = false;
        bool has_ineligible_prunable_child = false;
        ParallelReplicasEngagement certain_engagement;
        ParallelReplicasEngagement prunable_engagement;
        merge->hasChildTable([&](const StoragePtr & child)
        {
            has_children = true;
            auto child_engagement = mayEngageParallelReplicasForWrappedStorage(child, context);
            if (child->readsFromOtherTables())
            {
                certain_engagement.merge(child_engagement);
            }
            else
            {
                has_prunable_child = true;
                has_ineligible_prunable_child |= !child_engagement.any();
                prunable_engagement.merge(child_engagement);
            }
            return false;
        });
        if (!has_children)
            return {};

        ParallelReplicasEngagement children_engagement = certain_engagement;
        if (has_prunable_child && !has_ineligible_prunable_child)
        {
            /// The *flavor* of the prunable engagement has to survive pruning as well. A
            /// `Merge` whose children are all eligible can still mix flavors — say a view
            /// over a local `MergeTree` (`local_merge_tree`) and a view over a
            /// `Distributed` table (`remote`, both ordinary views are prunable) — and the
            /// two are not interchangeable: only a local `MergeTree` read runs the
            /// row-count estimate that may silently disable parallel replicas afterwards
            /// (see `ParallelReplicasEngagement`). Reporting `remote` for a child set the
            /// query may narrow to the local-`MergeTree` child alone would suppress that
            /// escape hatch and turn a query the plain planner runs into a
            /// `SUPPORT_IS_DISABLED` rejection. Which prunable children survive is not
            /// known before planning, so a mixed prunable set keeps only the
            /// estimate-subject flavor — unless a non-prunable child already engages
            /// remotely, which pruning cannot undo. For a query that does read the
            /// prunable remote child with the threshold set this under-throws — the same
            /// documented trade-off as above.
            if (prunable_engagement.remote
                && (prunable_engagement.local_merge_tree || certain_engagement.local_merge_tree))
                prunable_engagement.remote = false;

            /// The planner applies the min-rows estimate only when its candidate plan has
            /// exactly one `ReadFromMergeTree` step. Pruning can reduce the preflight's
            /// count, but only by removing prunable children, so keep the ambiguity with
            /// the engagement only when a prunable child contributes to a count of more
            /// than one.
            prunable_engagement.local_merge_tree_read_count_may_be_reduced_by_merge_pruning
                |= prunable_engagement.local_merge_tree_read_count > 0
                    && prunable_engagement.local_merge_tree_read_count
                            + certain_engagement.local_merge_tree_read_count
                        > 1;

            children_engagement.merge(prunable_engagement);
        }

        return children_engagement;
    }

    const auto * distributed = dynamic_cast<const StorageDistributed *>(&storage);
    if (!distributed)
        return {};

    auto cluster = distributed->getCluster();

    bool has_shard_with_several_nodes = false;
    bool has_single_node_shard = false;
    for (const auto & shard : cluster->getShardsInfo())
    {
        if (shard.getAllNodeCount() > 1)
            has_shard_with_several_nodes = true;
        else
            has_single_node_shard = true;
    }

    /// Every algorithm needs at least one shard with more than one node to split the read.
    if (!has_shard_with_several_nodes)
        return {};

    /// This is the table's full configured cluster, but the read path may shrink it before
    /// deciding on parallel replicas: `StorageDistributed::getQueryProcessingStage` applies
    /// `optimize_skip_unused_shards` (`getOptimizedCluster`) first, and
    /// `updateSettingsAndClientInfoForCluster` then judges the pruned cluster. When the full
    /// cluster mixes single-node and multi-node shards, pruning can leave single-node shards
    /// only, and the read silently runs without parallel replicas. Whether it does depends on
    /// the query's WHERE clause, which is not known before planning, so a storage whose read
    /// might be pruned that way must not count as eligible — otherwise the forcing mode would
    /// reject a query the plain read path would have run without parallel replicas anyway.
    /// Pruning is attempted only when the settings and the table allow it (the sharding key
    /// must exist and be usable for reads — the implicit `rand()` key of a `Remote` database
    /// proxy never prunes, see `hasShardingKeyForReads`), and never on the custom-key path,
    /// which works with the full cluster — mirror those conditions here.
    const auto & settings = context->getSettingsRef();
    if (has_single_node_shard && settings[Setting::optimize_skip_unused_shards] && distributed->hasShardingKeyForReads()
        && (settings[Setting::allow_nondeterministic_optimize_skip_unused_shards] || distributed->isShardingKeyDeterministic())
        && !context->canUseParallelReplicasCustomKeyForCluster(*cluster))
        return {};

    /// A `remote()` table function without a named cluster cannot use the task-based mode,
    /// but the custom-key and sampling modes do not go through `cluster_for_parallel_replicas`
    /// and work with the ad-hoc cluster.
    if (cluster->getName().empty() && !context->canUseParallelReplicasCustomKeyForCluster(*cluster)
        && !context->canUseOffsetParallelReplicas())
        return {};

    return {.local_merge_tree = false, .remote = true};
}

ParallelReplicasEngagement mayEngageParallelReplicas(IQueryTreeNode * root, const ContextPtr & scope_context);

/// Whether reading an ordinary `VIEW` that the analyzer did not inline could engage
/// parallel replicas.
///
/// With `analyzer_inline_views = 0` (the default) an ordinary view stays a `TableNode`
/// wrapping `StorageView` in the resolved query tree, and neither of the storage-level
/// checks sees through it: `StorageView::isRemote` is `false`, and the planner's rule
/// (`canUseTableForParallelReplicas`) unwraps a view only to an underlying `MergeTree`,
/// subject to `parallel_replicas_allow_view_over_mergetree`. Yet `StorageView::readImpl`
/// interprets the inner query with a context derived from the reading context (see
/// `getViewContext` in `StorageView.cpp`), so the parallel-replica settings pass through
/// it: a view defined over a `Distributed` table still reaches `ClusterProxy` and consults
/// them, and a view over an eligible `MergeTree` can engage them through that inner
/// interpretation even with `parallel_replicas_allow_view_over_mergetree = 0` (the setting
/// only gates the outer planner's unwrapping, and the disable in `getViewContext` is gated
/// on it too).
/// Decide the view's eligibility the way the planner's own view unwrapping does
/// (`StorageView::getUnderlyingMergeTreeStorageForParallelReplicas`): resolve the inner
/// query into a query tree — with the same context the read would use — and run the
/// same eligibility walk over it. Nested views recurse naturally through the walk.
ParallelReplicasEngagement mayEngageParallelReplicasForView(const StorageView & view, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context)
{
    /// A parameterized view cannot be resolved without its arguments. It also cannot
    /// really appear here: the analyzer resolves a parameterized-view invocation into a
    /// fresh `StorageView` with the parameters already substituted
    /// (`Context::buildParameterizedViewStorage`), for which this is `false`.
    if (view.isParameterizedView())
        return {};

    /// Resolving a query with an insertion-table context can poison the schema cache of
    /// table functions inside the view (`use_structure_from_insertion_table_in_table_functions`
    /// infers their structure from the insertion table) — the same guard as in
    /// `StorageView::getUnderlyingMergeTreeStorageForParallelReplicas`. The read itself
    /// decides then; nothing is checked ahead of it.
    if (context->hasInsertionTable())
        return {};

    auto view_context = Context::createCopy(StorageView::getViewSubqueryContext(context, storage_snapshot));

    /// This must mirror `StorageView::getViewContext`, which is the context used by
    /// `StorageView::readImpl`. When the outer planner unwraps a `VIEW` over an
    /// eligible `MergeTree`, the view's own read is local, even if a wrapper such as
    /// `Merge` or `Alias` leaves this preflight responsible for looking through it.
    Settings view_settings = view_context->getSettingsCopy();
    if (context->canUseParallelReplicasOnInitiator()
        && view_settings[Setting::parallel_replicas_allow_view_over_mergetree]
        && !view_settings[Setting::parallel_replicas_plan_based]
        && view.getUnderlyingMergeTreeStorageForParallelReplicas(context))
    {
        view_settings[Setting::allow_experimental_parallel_reading_from_replicas] = Field{0};
        view_context->setSettings(view_settings);
    }

    QueryTreeNodePtr inner_query_tree;
    try
    {
        inner_query_tree = buildQueryTree(storage_snapshot->metadata->getSelectQuery().inner_query->clone(), view_context);
        QueryTreePassManager pass_manager(view_context);
        addQueryTreePasses(pass_manager);
        pass_manager.runOnlyResolve(inner_query_tree);
    }
    catch (...) // Ok: reading the view resolves the same inner query with an equivalent context and will surface the same error, so treating the view as unable to engage parallel replicas does not silently downgrade anything.
    {
        /// A preflight failure must also not fail a recursive query whose steps never
        /// actually read the view (e.g. the recursion produces no rows).
        tryLogCurrentException(
            __PRETTY_FUNCTION__,
            fmt::format("Failed to resolve the inner query of view {} while checking parallel-replica eligibility", view.getStorageID().getFullTableName()),
            LogsLevel::trace);
        return {};
    }

    /// `QueryTreeBuilder` gives nested queries and UNION branches their own context copies.
    /// `mayEngageParallelReplicas` intentionally does not cross such a context boundary: a
    /// branch-local `SETTINGS` clause must not be judged using its parent's settings. Evaluate
    /// every context in the view tree separately, just as the recursive-query constructor does.
    /// Otherwise a view such as `SELECT * FROM (SELECT * FROM dist)` would stop at the nested
    /// query and incorrectly look unable to engage parallel replicas, even though
    /// `StorageView::readImpl` interprets that query with its own context.
    ParallelReplicasEngagement engagement;
    std::vector<IQueryTreeNode *> nodes_to_scan;
    nodes_to_scan.push_back(inner_query_tree.get());
    while (!nodes_to_scan.empty())
    {
        auto * node = nodes_to_scan.back();
        nodes_to_scan.pop_back();

        if (const auto * query_node = node->as<QueryNode>())
        {
            const auto & query_context = query_node->getContext();
            if (query_context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas])
                engagement.merge(mayEngageParallelReplicas(node, query_context));
        }
        else if (const auto * union_node = node->as<UnionNode>())
        {
            const auto & union_context = union_node->getContext();
            if (union_context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas])
                engagement.merge(mayEngageParallelReplicas(node, union_context));
        }

        for (auto & child : node->getChildren())
            if (child)
                nodes_to_scan.push_back(child.get());
    }

    return engagement;
}

/// Storage-level eligibility of a storage reached by unwrapping a delegating wrapper
/// (`StorageProxy` / materialized view target / `Alias` / `Buffer` / `Merge` child).
/// An ordinary `VIEW` must be judged with the view rule — `StorageView::isRemote` is
/// `false`, so an `isRemote` gate would silently miss a wrapper whose target is a view
/// over a remote table. Everything else goes through the remote-storage rule directly:
/// it returns `false` for local non-wrapper storages on its own, and gating it on
/// `isRemote` would wrongly stop at wrappers whose `isRemote` does not see the remote
/// source (`StorageMerge::isRemote` is `false` when the remote table hides behind a
/// view child, because `StorageView::isRemote` is `false`).
ParallelReplicasEngagement mayEngageParallelReplicasForWrappedStorage(const StoragePtr & storage, const ContextPtr & context)
{
    if (!storage)
        return {};

    if (const auto * view = typeid_cast<const StorageView *>(storage.get()))
    {
        const auto metadata_handle = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
        return mayEngageParallelReplicasForView(*view, storage->getStorageSnapshot(metadata_handle, context), context);
    }

    return mayEngageParallelReplicasForRemoteStorage(*storage, context);
}

/// Whether parallel replicas could actually be engaged for a query subtree.
///
/// A recursive step that reads nothing but local, non-`MergeTree` storages — in
/// particular a purely self-referential recursion such as
/// `SELECT n + 1 FROM t WHERE n < 10`, whose only table is the in-memory working
/// table — never engages parallel replicas in the planner: `findTableForParallelReplicas`
/// finds no eligible table, and the custom-key / sampling modes have nothing remote or
/// `MergeTree` to split either. Such a query must keep running as before, even under
/// the forcing mode, so the rejection below must not fire for it.
///
/// Eligibility is decided with the planner's own storage-level rule,
/// `canUseTableForParallelReplicas` (which unwraps views and materialized views subject
/// to `parallel_replicas_allow_view_over_mergetree` / `parallel_replicas_allow_materialized_views`
/// and then applies `isTableNodeEligibleForParallelReplicas`: `MergeTree` family, replicated
/// unless `parallel_replicas_for_non_replicated_merge_tree` is set, no `FINAL`), so that the
/// rejection below cannot be broader than the planner's decision. A plain local `MergeTree`
/// table with the default `parallel_replicas_for_non_replicated_merge_tree = 0`, for instance,
/// is not eligible and must keep running under the forcing mode.
///
/// Remote storages (`Distributed`, `remote`, `cluster`) are eligible in addition: they are not
/// covered by that rule (which only accepts the `MergeTree` family), yet every mode can split a
/// read across a cluster's replicas — but only for storages whose read actually goes through
/// the parallel-replica machinery and only when the cluster has a suitable shape, which is
/// what `mayEngageParallelReplicasForRemoteStorage` checks positively. Remote engines that
/// read directly (`MongoDB`, `MySQL`, ...) never consult the setting and are not eligible.
///
/// An ordinary non-inlined `VIEW` is eligible when its inner query is: the view's read
/// re-interprets that query with the reading context's settings, so a view over a remote
/// source can engage parallel replicas even though the view storage itself is not remote
/// (see `mayEngageParallelReplicasForView`).
///
/// The walk is scoped to `scope_context`: subqueries with a `SETTINGS` clause of their
/// own get their own context, and the settings that decide whether parallel replicas
/// are engaged are read from that context, not from `scope_context`. Descending into
/// them would attribute their tables to an unrelated context, so a branch-local
/// `SETTINGS allow_experimental_parallel_reading_from_replicas = 2` on a purely
/// self-referential branch would be rejected because of a sibling branch that reads a
/// `MergeTree` table. Nodes that share `scope_context` are the ones whose settings
/// really are the ones being examined, so only those are walked.

/// Mirror of `parallelReplicasEnabledForStorage` (`PlannerJoinTree.cpp`) for a `StorageView`:
/// the planner accepts a view as a parallel-replica read only when
/// `parallel_replicas_allow_view_over_mergetree` lets it unwrap the view to a `MergeTree`
/// table that is itself eligible (replicated, or non-replicated with
/// `parallel_replicas_for_non_replicated_merge_tree`).
bool parallelReplicasEnabledForViewStorage(const StorageView & view, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::parallel_replicas_allow_view_over_mergetree])
        return false;

    auto underlying_storage = view.getUnderlyingMergeTreeStorageForParallelReplicas(context);
    if (!underlying_storage)
        return false;

    if (!underlying_storage->isMergeTree())
        return false;

    return underlying_storage->supportsReplication() || settings[Setting::parallel_replicas_for_non_replicated_merge_tree];
}

/// Mirror the join-tree gates in `buildJoinTreeQueryPlan`
/// (`PlannerJoinTree.cpp`): `allowParallelReplicasForJoinTree` rejects a top-level
/// `CROSS JOIN` and a non-`ALL` `INNER JOIN`, while `should_disable_parallel_replicas`
/// silently sets
/// `enable_parallel_replicas = 0` before planning the reads — even under the forcing
/// mode — instead of throwing:
///   - a top-level `CROSS JOIN`, `FULL JOIN` or non-`ALL` `INNER JOIN` (only an
///     `ALL INNER`, `LEFT` or `RIGHT` join can drive parallel replicas at all),
///   - a top-level join whose leftmost table expression is a `VIEW` — either a plain
///     view table, or a `view(...)` table function that does not unwrap to an eligible
///     `MergeTree` table,
///   - an n-way join where a `LEFT`/`INNER`/`RIGHT` join precedes the last `RIGHT`
///     join (the left side of that `RIGHT` join cannot be parallelized),
///   - an n-way join involving a `FULL`, `GLOBAL` or `CROSS` join,
///   - a `RIGHT` join whose right side is a remote table (it gets wrapped into a
///     subquery, and parallel replicas would incorrectly pick the left table).
/// A query whose join tree matches one of these shapes never engages parallel
/// replicas in the planner, so the forced-mode rejection must not fire for it
/// either: the plain planner runs the equivalent non-recursive query without them,
/// and the recursive step has to run plainly too — the disable it falls through to
/// is exactly the planner's own silent disable. The remote check matches the
/// planner's, which reads `TableExpressionData::isRemote` — set from
/// `IStorage::isRemote` for table and table-function expressions and left `false`
/// for any other right side (e.g. a subquery).
bool plannerDisablesParallelReplicasForJoinTreeShape(const QueryTreeNodePtr & join_tree_node, const ContextPtr & context)
{
    /// `allowParallelReplicasForJoinTree` only permits a top-level `INNER JOIN`
    /// with `ALL` strictness and rejects a top-level `CROSS JOIN`. The engagement
    /// walk below evaluates storage eligibility per leaf, so it must apply this
    /// join-tree eligibility before any eligible leaf can make forced mode throw.
    if (join_tree_node->as<CrossJoinNode>())
        return true;

    if (const auto * join_node = join_tree_node->as<JoinNode>())
    {
        /// `allowParallelReplicasForJoinTree` rejects an ordinary `VIEW` as the
        /// leftmost join-table expression. `StorageView::readImpl` can still use
        /// parallel replicas for the view's inner query, but that query does not
        /// reference the recursive working table, so it cannot use the stale
        /// `GLOBAL JOIN` table that this guard prevents.
        const auto & left_table_expression = join_node->getLeftTableExpressionNode();
        if (const auto * left_table = left_table_expression->as<TableNode>();
            left_table && left_table->getStorage()->isView())
            return true;

        /// The same escape for a `StorageView` reached through a table function
        /// (`view(...)`, `viewIfPermitted(...)`): `allowParallelReplicasForJoinTree`
        /// does not apply its unconditional `TableNode` view rejection to it, but
        /// falls back to the storage-level rule for the leftmost table expression,
        /// which is `parallelReplicasEnabledForStorage`. That rule accepts a view
        /// only when it unwraps to an eligible `MergeTree` table, so a `view(...)`
        /// over a `Distributed` table (or over a `MergeTree` table the settings do
        /// not allow) leaves the outer join tree ineligible, exactly like a plain
        /// view table. A view that does unwrap to an eligible `MergeTree` table
        /// stays eligible here, so a genuine engagement is still reported.
        if (const auto * left_table_function = left_table_expression->as<TableFunctionNode>())
        {
            if (const auto * view = typeid_cast<const StorageView *>(left_table_function->getStorage().get());
                view && !parallelReplicasEnabledForViewStorage(*view, context))
                return true;
        }

        /// `allowParallelReplicasForJoinTree` only ever allows an `ALL INNER`, a
        /// `LEFT` or a `RIGHT` join to drive parallel replicas; every other join
        /// kind — in particular a single `FULL JOIN`, which the n-way rules below
        /// do not cover — falls through to its final `return false`.
        const auto join_kind = join_node->getKind();
        if (!(join_kind == JoinKind::Inner && join_node->getStrictness() == JoinStrictness::All)
            && join_kind != JoinKind::Left
            && join_kind != JoinKind::Right)
            return true;
    }

    /// Post-order like `buildTableExpressionsStack`, but tolerant of unresolved
    /// trees: the engagement walk also inspects view inner queries built by
    /// `QueryTreeBuilder` without analysis, whose table expressions are still
    /// `IDENTIFIER` nodes (`buildTableExpressionsStack` would throw on them) —
    /// any node that is not a join is a plain leaf table expression here.
    QueryTreeNodes table_expressions_stack;
    std::function<void(const QueryTreeNodePtr &)> collect_table_expressions = [&](const QueryTreeNodePtr & node)
    {
        switch (node->getNodeType())
        {
            case QueryTreeNodeType::ARRAY_JOIN:
                collect_table_expressions(node->as<ArrayJoinNode &>().getTableExpressionNode());
                break;
            case QueryTreeNodeType::CROSS_JOIN:
                for (const auto & expr : node->as<CrossJoinNode &>().getTableExpressions())
                    collect_table_expressions(expr);
                break;
            case QueryTreeNodeType::JOIN:
            {
                auto & join = node->as<JoinNode &>();
                collect_table_expressions(join.getLeftTableExpressionNode());
                collect_table_expressions(join.getRightTableExpressionNode());
                break;
            }
            default:
                break;
        }
        table_expressions_stack.push_back(node);
    };
    collect_table_expressions(join_tree_node);

    size_t joins_count = 0;
    bool is_full_join = false;
    bool is_global_join = false;
    bool is_cross_join = false;
    bool is_right_join_with_remote_table = false;
    int first_join_pos = -1;
    int last_right_join_pos = -1;

    for (size_t i = 0; i < table_expressions_stack.size(); ++i)
    {
        const auto & table_expression = table_expressions_stack[i];
        const auto node_type = table_expression->getNodeType();

        if (node_type == QueryTreeNodeType::CROSS_JOIN)
        {
            joins_count += table_expression->as<const CrossJoinNode &>().getTableExpressions().size() - 1;
            is_cross_join = true;
            continue;
        }

        if (node_type != QueryTreeNodeType::JOIN)
            continue;

        ++joins_count;
        const auto & join_node = table_expression->as<const JoinNode &>();
        const auto join_kind = join_node.getKind();

        if (join_kind == JoinKind::Full)
            is_full_join = true;

        if (join_node.getLocality() == JoinLocality::Global)
            is_global_join = true;

        if (first_join_pos < 0 && (join_kind == JoinKind::Left || join_kind == JoinKind::Inner || join_kind == JoinKind::Right))
            first_join_pos = static_cast<int>(i);

        if (join_kind == JoinKind::Right)
        {
            last_right_join_pos = static_cast<int>(i);

            const auto & right_table_expression = join_node.getRightTableExpressionNode();
            StoragePtr right_storage;
            if (const auto * right_table_node = right_table_expression->as<TableNode>())
                right_storage = right_table_node->getStorage();
            else if (const auto * right_table_function_node = right_table_expression->as<TableFunctionNode>())
                right_storage = right_table_function_node->getStorage();
            is_right_join_with_remote_table = right_storage && right_storage->isRemote();
        }
    }

    /// n-way join like LEFT/INNER/RIGHT ... RIGHT ...
    if (first_join_pos >= 0 && last_right_join_pos >= 0 && first_join_pos < last_right_join_pos)
        return true;

    /// n-way join with FULL JOIN or GLOBAL JOIN or CROSS JOIN
    if (joins_count > 1 && (is_full_join || is_global_join || is_cross_join))
        return true;

    /// RIGHT JOIN with a remote table on the right side
    return is_right_join_with_remote_table;
}

ParallelReplicasEngagement mayEngageParallelReplicas(IQueryTreeNode * root, const ContextPtr & scope_context)
{
    ParallelReplicasEngagement engagement;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty() && !(engagement.local_merge_tree && engagement.remote))
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (subtree_node != root)
        {
            const Context * node_context = nullptr;
            if (const auto * query_node = subtree_node->as<QueryNode>())
                node_context = query_node->getContext().get();
            else if (const auto * union_node = subtree_node->as<UnionNode>())
                node_context = union_node->getContext().get();

            if (node_context && node_context != scope_context.get())
                continue;
        }

        /// A `QueryNode` whose own join tree matches a shape the planner silently
        /// disables parallel replicas for (see
        /// `plannerDisablesParallelReplicasForJoinTreeShape`) contributes no
        /// engagement from that join tree: none of the reads planned for it can
        /// engage them, whatever tables it contains. Its other children (the
        /// projection, `WHERE`, ...) are still walked — subqueries there are
        /// planned separately and are not covered by the join-tree disable.
        if (const auto * query_node = subtree_node->as<QueryNode>())
        {
            const auto & join_tree = query_node->getJoinTreeNode();
            if (join_tree && plannerDisablesParallelReplicasForJoinTreeShape(join_tree, scope_context))
            {
                for (auto & child : subtree_node->getChildren())
                    if (child && child != join_tree)
                        nodes_to_process.push_back(child.get());
                continue;
            }
        }

        if (const auto * table_node = subtree_node->as<TableNode>())
        {
            /// Not gated on `isRemote`: the rule itself returns `false` for local
            /// non-delegating storages, while a delegating wrapper must be unwrapped even
            /// when it does not report itself remote — `Merge` and `Alias` forward
            /// `isRemote` to their targets, and a target that is an ordinary `VIEW` over a
            /// remote table is not remote itself yet can engage parallel replicas.
            const auto & storage = table_node->getStorage();
            if (storage)
                engagement.merge(mayEngageParallelReplicasForRemoteStorage(*storage, scope_context));

            ParallelReplicasEngagement view_engagement;
            if (const auto * view = typeid_cast<const StorageView *>(storage.get()))
                view_engagement = mayEngageParallelReplicasForView(*view, table_node->getStorageSnapshot(), scope_context);

            if (canUseTableForParallelReplicas(*table_node, scope_context))
            {
                engagement.local_merge_tree = true;
                /// When the planner uses the outer view as the candidate read, its inner
                /// query has parallel replicas disabled and contributes no read here. If
                /// the inner query can engage them instead, it already supplies the
                /// precise count (including a `UNION ALL` with multiple reads), so do not
                /// count the same view twice.
                if (!view_engagement.local_merge_tree_read_count)
                    ++engagement.local_merge_tree_read_count;
            }

            engagement.merge(view_engagement);
        }
        else if (const auto * table_function_node = subtree_node->as<TableFunctionNode>())
        {
            /// A table function resolving to a remote storage (`remote`, `cluster`) can be read
            /// with parallel replicas; local ones (`numbers`, `file`, ...) never are — the
            /// planner rejects a `TABLE_FUNCTION` join-tree node outright. The `view` table
            /// function and a parameterized-view invocation resolve to a `StorageView`, whose
            /// read re-interprets the inner query — check it like a view table.
            const auto & storage = table_function_node->getStorage();
            if (storage)
                engagement.merge(mayEngageParallelReplicasForRemoteStorage(*storage, scope_context));

            if (const auto * view = typeid_cast<const StorageView *>(storage.get()))
            {
                ParallelReplicasEngagement view_engagement
                    = mayEngageParallelReplicasForView(*view, table_function_node->getStorageSnapshot(), scope_context);

                /// Mirror of the `TableNode` branch above. The planner decides a table-function
                /// leaf with the storage-level rule (`parallelReplicasEnabledForStorage`), which
                /// accepts a `StorageView` when it unwraps to an eligible `MergeTree` table —
                /// then the outer read is the local `MergeTree` read (subject to the row-count
                /// estimate), and the view's inner query has parallel replicas disabled by
                /// `StorageView::getViewContext`, contributing no read of its own. Reporting
                /// only the inner-query engagement here would leave a leftmost
                /// `view(...)` over an eligible `MergeTree` table looking unable to engage
                /// parallel replicas, silently downgrading the forcing mode for a step the
                /// planner would really parallelize.
                if (parallelReplicasEnabledForViewStorage(*view, scope_context))
                {
                    engagement.local_merge_tree = true;
                    /// Do not count the same view twice (see the `TableNode` branch).
                    if (!view_engagement.local_merge_tree_read_count)
                        ++engagement.local_merge_tree_read_count;
                }

                engagement.merge(view_engagement);
            }
        }

        for (auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return engagement;
}

/// Equi-join key between the recursive CTE working table and a real table,
/// tagged with the `QueryNode` whose join tree contains the join. The filter
/// will be injected into that `QueryNode`'s `WHERE` clause.
struct CTEJoinKey
{
    QueryNode * containing_query_node;
    String cte_column_name;
    DataTypePtr cte_column_type;
    ColumnNode * real_column_node;
};

bool isCTETableNode(const IQueryTreeNode * node, const std::vector<TableNode *> & recursive_table_nodes)
{
    for (const auto * table_node : recursive_table_nodes)
        if (node == table_node)
            return true;
    return false;
}

/// Extract equi-join key pairs from an `ON` join expression.
/// Handles single `equals` and `AND`-combined conditions.
void extractEquiJoinKeys(
    const QueryTreeNodePtr & expression,
    const std::vector<TableNode *> & recursive_table_nodes,
    QueryNode & containing_query_node,
    std::vector<CTEJoinKey> & result)
{
    const auto * function_node = expression->as<FunctionNode>();
    if (!function_node)
        return;

    if (function_node->getFunctionName() == "and")
    {
        for (const auto & arg : function_node->getArguments().getNodes())
            extractEquiJoinKeys(arg, recursive_table_nodes, containing_query_node, result);
        return;
    }

    if (function_node->getFunctionName() != "equals")
        return;

    const auto & args = function_node->getArguments().getNodes();
    if (args.size() != 2)
        return;

    auto * left_column = args[0]->as<ColumnNode>();
    auto * right_column = args[1]->as<ColumnNode>();
    if (!left_column || !right_column)
        return;

    auto left_source = left_column->getColumnSourceOrNull();
    auto right_source = right_column->getColumnSourceOrNull();
    if (!left_source || !right_source)
        return;

    bool left_is_cte = isCTETableNode(left_source.get(), recursive_table_nodes);
    bool right_is_cte = isCTETableNode(right_source.get(), recursive_table_nodes);

    /// Both columns from the same side (both CTE or both real) — skip.
    if (left_is_cte == right_is_cte)
        return;

    auto * cte_column = left_is_cte ? left_column : right_column;
    auto * real_column = left_is_cte ? right_column : left_column;
    const auto & real_source = left_is_cte ? right_source : left_source;

    /// Real side must be a physical table — filter pushdown only makes sense
    /// against a storage's primary key.
    if (!real_source->as<TableNode>())
        return;

    result.push_back({&containing_query_node, cte_column->getColumnName(), cte_column->getColumnType(), real_column});
}

/// Walk the join tree of a single `QueryNode` to collect equi-join keys.
///
/// The collected predicate is later injected into the `QueryNode`'s `WHERE`,
/// which applies after every join. That is semantics-preserving only when the
/// matched inner join is not nested on the nullable side of an outer join: a
/// `LEFT`/`RIGHT`/`FULL` join produces null-extended rows for unmatched outer
/// rows, and `real_column IN (...)` at `WHERE`-level would evaluate to NULL
/// (i.e. false) for those rows and silently drop them. To stay correct, the
/// walk tracks whether the current subtree sits on a nullable side and skips
/// inner joins reached through such a path.
void collectCTEJoinKeysInQuery(
    QueryNode & query_node,
    const std::vector<TableNode *> & recursive_table_nodes,
    std::vector<CTEJoinKey> & result)
{
    struct StackEntry
    {
        IQueryTreeNode * node;
        bool in_nullable_position;
    };

    std::vector<StackEntry> nodes_to_visit;
    nodes_to_visit.push_back({query_node.getJoinTreeNode().get(), false});

    while (!nodes_to_visit.empty())
    {
        auto entry = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        auto * join_node = entry.node->as<JoinNode>();
        if (!join_node)
            continue;

        const auto kind = join_node->getKind();

        /// Only `ON` equi-joins are handled. `JOIN ... USING (c)` is intentionally
        /// skipped: `USING` forces the recursive working-table column and the real
        /// table's join column to share the name `c`, and a recursive CTE whose
        /// working column collides with a joined table's column name currently
        /// evaluates to an empty result in ClickHouse regardless of this
        /// optimization (the recursion produces no rows, not even the anchor). The
        /// optimization is therefore scoped to `ON`/comma equi-joins, which is what
        /// the changelog and tests claim.
        if (kind == JoinKind::Inner
            && join_node->hasJoinExpression()
            && join_node->isOnJoinExpression()
            && !entry.in_nullable_position)
        {
            extractEquiJoinKeys(join_node->getJoinExpression(), recursive_table_nodes, query_node, result);
        }

        const bool left_nullable = entry.in_nullable_position || kind == JoinKind::Right || kind == JoinKind::Full;
        const bool right_nullable = entry.in_nullable_position || kind == JoinKind::Left || kind == JoinKind::Full;

        nodes_to_visit.push_back({join_node->getLeftTableExpressionNode().get(), left_nullable});
        nodes_to_visit.push_back({join_node->getRightTableExpressionNode().get(), right_nullable});
    }
}

/// Collect all CTE join keys in the recursive query. When the recursive query
/// is a `UnionNode`, every branch is inspected independently so that filters
/// can later be injected into each branch's `WHERE` in isolation.
std::vector<CTEJoinKey> collectCTEJoinKeys(
    IQueryTreeNode & recursive_query,
    const std::vector<TableNode *> & recursive_table_nodes)
{
    std::vector<CTEJoinKey> result;

    if (auto * query_node = recursive_query.as<QueryNode>())
    {
        collectCTEJoinKeysInQuery(*query_node, recursive_table_nodes, result);
    }
    else if (auto * union_node = recursive_query.as<UnionNode>())
    {
        for (auto & subquery : union_node->getQueries().getNodes())
        {
            if (auto * sub_query_node = subquery->as<QueryNode>())
                collectCTEJoinKeysInQuery(*sub_query_node, recursive_table_nodes, result);
        }
    }

    return result;
}

/// Read deduplicated values of a column from a `StorageMemory`-backed temporary
/// table. Returns nullopt — so the caller skips filter injection for the step
/// and falls back to a plain scan — when the generated set would be too large
/// to build safely:
///  - the number of distinct values exceeds `max_cardinality`, or
///  - the accumulated byte size of the distinct values exceeds `max_bytes`
///    (the effective `max_bytes_in_set`; `0` means unlimited).
///
/// The byte budget bounds the work *while the values are being collected*, so a
/// frontier of wide keys under a tight `max_bytes_in_set` fails closed before
/// the full set is materialized here and re-materialized as the RHS tuple — the
/// unoptimized recursive scan never builds either, so it must not pay for them.
/// The check is conservative: it measures the unconverted key bytes, an upper
/// bound on the set the planner builds after converting to the storage column
/// type, so it can only skip the optimization earlier, never inject an oversized
/// set (the exact post-conversion check in `generatedInSetIsSafeToInject` still
/// runs for everything that passes here).
std::optional<std::vector<Field>> readColumnValuesFromMemoryStorage(
    const StoragePtr & storage,
    const String & column_name,
    const ContextPtr & context,
    size_t max_cardinality,
    size_t max_bytes)
{
    auto * memory_storage = typeid_cast<StorageMemory *>(storage.get());
    if (!memory_storage)
        return std::vector<Field>{};

    auto metadata = memory_storage->getInMemoryMetadataPtr(context, false);
    auto snapshot = memory_storage->getStorageSnapshot(metadata, context);
    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*snapshot->data);

    if (!snapshot_data.blocks)
        return std::vector<Field>{};

    /// Deduplicate by the value's serialized raw representation, NOT by
    /// `Field` ordering: `Field` compares floats SQL-style (`+0.` and `-0.`
    /// are equal, and all NaNs are equivalent regardless of payload), while
    /// the hash `JOIN` this filter guards and the generated `IN` set both
    /// match keys on the raw representation (`-0. IN (0.)` is `0`, and a hash
    /// join does not match a `-0.` probe against a `+0.` build key). A
    /// `Field`-based dedup of a frontier holding both `+0.` and `-0.` would
    /// keep only one of them, and the injected `IN` prefilter would then drop
    /// the row that joins with the other — losing a recursion branch.
    /// `serializeValueIntoArena` is the same raw representation generic
    /// hash-join and aggregation keys use, so raw-distinct values stay
    /// distinct here exactly when the join distinguishes them.
    Arena arena;
    std::unordered_set<std::string_view> unique_values;
    std::vector<Field> values;
    size_t accumulated_bytes = 0;

    for (const auto & block : *snapshot_data.blocks)
    {
        if (!block.has(column_name))
            continue;

        const auto & column = block.getByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i)
        {
            const char * begin = nullptr;
            const auto serialized = column->serializeValueIntoArena(i, arena, begin, nullptr);
            if (!unique_values.insert(serialized).second)
            {
                /// A duplicate: return its serialized bytes to the arena (only
                /// the most recent allocation is rolled back, so the stored
                /// view of the first occurrence stays valid).
                arena.rollback(serialized.size());
                continue;
            }

            values.push_back((*column)[i]);

            accumulated_bytes += column->byteSizeAt(i);
            if (max_bytes != 0 && accumulated_bytes > max_bytes)
                return std::nullopt;

            if (values.size() > max_cardinality)
                return std::nullopt;
        }
    }

    return values;
}

/// Build the RHS tuple constant for the generated `IN`.
///
/// The tuple elements are typed using the CTE column's type (the type the
/// values were originally produced with), not the real column's type. This
/// matches the semantics of `JOIN ... ON real_col = cte_col`: the join is
/// resolved over a common comparison type, and values that are valid under
/// the join but not representable in the storage column's type (e.g.
/// `Int64(-1)` against a `UInt8` column, or `NULL` against a non-nullable
/// column) are correctly evaluated as no-match rather than triggering a
/// conversion exception while the filter is being built.
std::shared_ptr<ConstantNode> buildInRhsConstantNode(const DataTypePtr & cte_column_type, const std::vector<Field> & values)
{
    Tuple tuple_values;
    tuple_values.reserve(values.size());
    DataTypes tuple_element_types;
    tuple_element_types.reserve(values.size());

    for (const auto & value : values)
    {
        tuple_values.push_back(value);
        tuple_element_types.push_back(cte_column_type);
    }

    return std::make_shared<ConstantNode>(
        Field(std::move(tuple_values)),
        std::make_shared<DataTypeTuple>(std::move(tuple_element_types)));
}

/// Build a resolved query-tree expression equivalent to `real_column IN (rhs...)`.
QueryTreeNodePtr buildInFilterNode(
    ColumnNode & real_column,
    std::shared_ptr<ConstantNode> rhs_node,
    const ContextPtr & context)
{
    auto in_function_node = std::make_shared<FunctionNode>("in");
    in_function_node->markAsOperator();
    in_function_node->getArguments().getNodes() = {real_column.clone(), std::move(rhs_node)};
    resolveOrdinaryFunctionNodeByName(*in_function_node, "in", context);

    return in_function_node;
}

/// Returns true if the planner can safely build and inject the set for
/// `real_column IN (rhs...)`: the conversion `CollectSets` will apply does not
/// throw, and the resulting set stays within the user's configured set-size
/// limits (`max_rows_in_set` / `max_bytes_in_set`). When it returns false the
/// caller skips injection for this step and the physical table is scanned
/// without the generated predicate, exactly as if the optimization were
/// disabled.
///
/// The injected predicate is only an optimization, so it must never change the
/// observable behaviour of the recursive query. There are three ways it could,
/// and all are guarded here by failing closed:
///
///  - Size limits. The planner lowers the injected `IN` through
///    `FutureSetFromTuple`, which enforces these limits via
///    `PreparedSets::getSizeLimitsForSet`: under `set_overflow_mode = 'throw'`
///    an oversized set raises `SET_SIZE_LIMIT_EXCEEDED`, and under `'break'` it
///    is silently truncated. Either outcome diverges from the unoptimized scan
///    (fail, or return incomplete results).
///
///  - Conversion failure. `CollectSets` converts the RHS constant to the `IN`
///    left-hand side's type — the joined real column's type, not the CTE column
///    type the tuple elements carry — via `getSetElementsForConstantValue`, and
///    that conversion can itself throw. For example a recursive `String` key
///    joined against an `Enum` column with `validate_enum_literals_in_operators
///    = 1` raises `UNKNOWN_ELEMENT_OF_ENUM` for a frontier value that the
///    original `enum_col = cte_string` comparison would simply treat as a
///    no-match. The conversion must therefore be attempted here unconditionally
///    (not only when size limits are set) and any exception from it treated as
///    "do not inject".
///
///  - Memory. Materializing the set can raise `MEMORY_LIMIT_EXCEEDED` under a
///    tight `max_memory_usage` even when the row/byte set limits are unlimited
///    (the default). The unoptimized scan never builds this set, so the probe is
///    always built here (through the same memory tracker) and any such failure is
///    treated as "do not inject" — see the guarded build below.
///
/// The set is measured exactly the way the planner would build it: the same
/// conversion of the same constant, then a `Set` built the way
/// `FutureSetFromTuple` builds it. The conversion changes both the row count
/// (non-representable values are dropped as no-match) and the per-row byte size
/// (e.g. a `UInt8` CTE key joined against a `UInt64` storage key), so measuring
/// after conversion is what makes the size decision exact.
bool generatedInSetIsSafeToInject(
    const DataTypePtr & real_column_type,
    const std::shared_ptr<ConstantNode> & rhs_node,
    const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    const size_t max_rows = settings[Setting::max_rows_in_set];
    const size_t max_bytes = settings[Setting::max_bytes_in_set];

    ColumnsWithTypeAndName set_columns;
    try
    {
        set_columns = getSetElementsForConstantValue(
            real_column_type,
            rhs_node->getColumn(),
            rhs_node->getResultType(),
            GetSetElementParams{
                .transform_null_in = settings[Setting::transform_null_in],
                .forbid_unknown_enum_values = settings[Setting::validate_enum_literals_in_operators],
            });
    }
    catch (...)
    {
        /// The planner's own conversion in `CollectSets` would throw the same
        /// way, failing the whole recursive query. Fail closed: skip injection
        /// and let the step fall back to a plain scan.
        return false;
    }

    /// The probe set is built below even when both set-size limits are unlimited
    /// (`0`, the default). Measuring it against the limits is then unnecessary,
    /// but building it is still required to fail closed: the planner later
    /// materializes the real `IN` set through `FutureSetFromTuple`, and that build
    /// can raise `MEMORY_LIMIT_EXCEEDED` under a tight `max_memory_usage` even
    /// with unlimited row/byte set limits. The probe below allocates through the
    /// same memory tracker, so if the real set would not fit, the probe build hits
    /// the limit here and we skip injection (plain scan) instead of turning a
    /// query the unoptimized scan would have run into an exception.
    ColumnsWithTypeAndName header = set_columns;
    for (auto & elem : header)
        elem.column = elem.column->cloneEmpty();

    Columns columns;
    columns.reserve(set_columns.size());
    for (const auto & elem : set_columns)
        columns.push_back(elem.column);

    /// Build the set the way `FutureSetFromTuple` would, then compare its
    /// measured size against the user's limits using the same `>` boundary the
    /// `throw` path uses, so the decision is exact for both overflow modes.
    ///
    /// The probe is built with unlimited `SizeLimits` so its full size can be
    /// measured exactly, but building it must itself fail closed. The frontier
    /// holds up to `recursive_cte_max_in_filter_cardinality` values, so with
    /// large `String` keys and a tight `max_memory_usage` materializing the
    /// hash table can hit `MEMORY_LIMIT_EXCEEDED` — the memory tracker fires
    /// during the build, before the comparison below. The unoptimized scan
    /// never builds this set, so such a failure must skip injection (plain
    /// scan), not fail the recursive query — exactly like the conversion guard
    /// above.
    try
    {
        Set set(SizeLimits{}, 0, settings[Setting::transform_null_in]);
        set.setHeader(header);
        set.insertFromColumns(columns);
        set.finishInsert();

        if (max_rows != 0 && set.getTotalRowCount() > max_rows)
            return false;
        if (max_bytes != 0 && set.getTotalByteCount() > max_bytes)
            return false;
    }
    catch (...) // Ok: building the probe set hit a limit (e.g. memory); skip injection and fall back to a plain scan instead of failing the recursive query.
    {
        return false;
    }
    return true;
}

/// Returns true if `type` is or contains (e.g. inside `Nullable`,
/// `LowCardinality`, `Array`, `Tuple`, `Map`) a floating-point type
/// (`Float32` / `Float64` / `BFloat16`).
bool typeInvolvesFloatingPoint(const DataTypePtr & type)
{
    if (WhichDataType(*type).isFloat())
        return true;

    bool result = false;
    type->forEachChild([&](const IDataType & child)
    {
        if (WhichDataType(child).isFloat())
            result = true;
    });
    return result;
}

/// Returns true if any of the enabled join algorithms compares keys by value
/// through `compareAt` rather than matching them on the raw representation.
/// For floating-point keys the two disagree: `compareAt` treats `+0.` and
/// `-0.` as equal and all NaNs as equal, while the raw representation (used
/// by the hash-family joins — `hash`, `parallel_hash`, `grace_hash`,
/// `direct` — and by the generated `IN` set) distinguishes both. `auto` is
/// value-comparing because it may fall back to `partial_merge` at runtime.
/// `parallel_full_sorting_merge` builds the very same `FullSortingMergeJoin`
/// as `full_sorting_merge`, so it is value-comparing as well.
bool joinAlgorithmsMayCompareFloatsByValue(const std::vector<JoinAlgorithm> & join_algorithms)
{
    for (const auto algorithm : join_algorithms)
    {
        if (algorithm == JoinAlgorithm::AUTO
            || algorithm == JoinAlgorithm::PARTIAL_MERGE
            || algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE
            || algorithm == JoinAlgorithm::FULL_SORTING_MERGE
            || algorithm == JoinAlgorithm::PARALLEL_FULL_SORTING_MERGE)
            return true;
    }
    return false;
}

/// Conjoin a list of predicate nodes into a single `and(...)` expression.
QueryTreeNodePtr conjoinPredicates(std::vector<QueryTreeNodePtr> predicates, const ContextPtr & context)
{
    if (predicates.empty())
        return nullptr;
    if (predicates.size() == 1)
        return std::move(predicates.front());

    auto and_function_node = std::make_shared<FunctionNode>("and");
    and_function_node->markAsOperator();
    and_function_node->getArguments().getNodes() = std::move(predicates);
    resolveOrdinaryFunctionNodeByName(*and_function_node, "and", context);
    return and_function_node;
}

}

class RecursiveCTEChunkGenerator
{
public:
    RecursiveCTEChunkGenerator(SharedHeader header_, QueryTreeNodePtr recursive_cte_union_node_)
        : header(std::move(header_))
        , recursive_cte_union_node(std::move(recursive_cte_union_node_))
    {
        auto & recursive_cte_union_node_typed = recursive_cte_union_node->as<UnionNode &>();
        chassert(recursive_cte_union_node_typed.hasRecursiveCTETable());

        auto & recursive_cte_table = recursive_cte_union_node_typed.getRecursiveCTETable();

        const auto & cte_name = recursive_cte_union_node_typed.getCTEName();
        recursive_table_nodes = collectTableNodesWithTemporaryTableName(cte_name, recursive_cte_union_node.get());
        if (recursive_table_nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION query {} is not recursive", recursive_cte_union_node->formatASTForErrorMessage());

        size_t recursive_cte_union_node_queries_size = recursive_cte_union_node_typed.getQueries().getNodes().size();
        chassert(recursive_cte_union_node_queries_size > 1);

        non_recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[0];
        recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[1];

        if (recursive_cte_union_node_queries_size > 2)
        {
            auto working_union_query = std::make_shared<UnionNode>(recursive_cte_union_node_typed.getMutableContext(),
                recursive_cte_union_node_typed.getUnionMode());
            auto & working_union_query_subqueries = working_union_query->getQueries().getNodes();

            for (size_t i = 1; i < recursive_cte_union_node_queries_size; ++i)
                working_union_query_subqueries.push_back(recursive_cte_union_node_typed.getQueries().getNodes()[i]);

            recursive_query = std::move(working_union_query);
        }

        /// Disable parallel replicas in every `QueryNode`/`UnionNode` of the recursive query.
        /// When parallel replicas is enabled, the planner rewrites JOINs to GLOBAL JOIN and
        /// materializes the right-side subquery into a cached external table keyed by tree
        /// hash. Across recursive steps the tree structure is identical (only the working
        /// table's data changes), so the cache key collides and stale data is reused —
        /// producing wrong results. The planner reads this setting from each node's
        /// `mutable_context` (see `Planner::buildPlannerContext`), not from the outer
        /// interpreter context, so overriding only the interpreter context is not enough.
        /// We rewrite the contexts once at construction time and preserve sharing: nodes
        /// that originally pointed to the same context will share the same copy afterwards.
        /// Whether parallel replicas could be engaged is a property of a context, not of the
        /// whole recursive query: a branch with its own `SETTINGS` clause has its own context,
        /// and the throw below is evaluated per context. Precompute the property for every
        /// context in the recursive query, so that the result does not depend on the order in
        /// which the nodes sharing a context happen to be visited. A context counts as
        /// parallel-replica-eligible if any node using it may engage parallel replicas.
        std::map<const Context *, ParallelReplicasEngagement> context_may_engage_parallel_replicas;
        {
            std::vector<IQueryTreeNode *> nodes_to_scan;
            nodes_to_scan.push_back(recursive_query.get());
            while (!nodes_to_scan.empty())
            {
                auto * node = nodes_to_scan.back();
                nodes_to_scan.pop_back();

                ContextPtr node_context;
                if (const auto * qn = node->as<QueryNode>())
                    node_context = qn->getContext();
                else if (const auto * un = node->as<UnionNode>())
                    node_context = un->getContext();

                if (node_context)
                    context_may_engage_parallel_replicas[node_context.get()].merge(mayEngageParallelReplicas(node, node_context));

                for (auto & child : node->getChildren())
                    if (child)
                        nodes_to_scan.push_back(child.get());
            }
        }

        std::map<Context *, ContextMutablePtr> context_copies;
        auto rewrite_context = [&context_copies, &context_may_engage_parallel_replicas](ContextMutablePtr & ctx)
        {
            if (auto it = context_copies.find(ctx.get()); it != context_copies.end())
            {
                ctx = it->second;
                return;
            }

            /// The stale cache guarded here is specific to the legacy SQL-shipping
            /// construction: `rewriteJoinToGlobalJoin` + `buildQueryTreeForShard`
            /// materialize the working table into an external `_data_<tree hash>`
            /// table and reuse it whenever the hash matches — and across recursive
            /// steps the tree is identical, only the working table's data changes.
            /// With `parallel_replicas_plan_based` that construction never runs for
            /// a recursive step: both places that invoke it under the analyzer
            /// (`Planner::buildPlanForQueryNode` and the read-step replacement in
            /// `PlannerJoinTree`) skip it in favor of building a plain local plan
            /// and distributing a serialized plan fragment later
            /// (`QueryPlanOptimizations::applyParallelReplicas`), whose remote
            /// executor ships the *current* external tables with every fragment
            /// instead of consulting a hash-keyed cache. The remaining legacy
            /// callers (`StorageMergeTree::read`,
            /// `StorageReplicatedMergeTree::read`) run only without the analyzer,
            /// which recursive CTEs require. The custom-key and sampling/offset
            /// modes never perform the `GLOBAL JOIN` rewrite in the first place
            /// (`executeQueryWithParallelReplicasCustomKey`), and keep their own
            /// planner-level forced-mode rejections (e.g. "JOINs are not supported
            /// with parallel replicas"). So under the plan-based mode the hazard is
            /// absent for every algorithm: leave the recursive-step contexts
            /// untouched, preserving both plan-based parallelism (mode `1`) and the
            /// force-or-throw contract (mode `2`) exactly as for a non-recursive
            /// query.
            if (ctx->getSettingsRef()[Setting::parallel_replicas_plan_based])
                return;

            /// `allow_experimental_parallel_reading_from_replicas = 2` is the
            /// forcing mode, documented as "enabled, throw an exception in case
            /// of failure". Recursive steps cannot use parallel replicas (the
            /// stale GLOBAL JOIN cache described above would return wrong
            /// results), so the request cannot be honoured. Silently
            /// downgrading to a plain run would break that force-or-throw
            /// contract, so fail closed with a clear error rather than pretend
            /// it succeeded.
            ///
            /// The throw is gated on parallel replicas actually being usable for
            /// this context, so it fires under the same condition as the forced
            /// mode rejections in the planner (e.g. FINAL / JOIN / IN-subquery in
            /// `Planner::buildPlanForQueryNode`): only when parallel replicas
            /// would actually be engaged. A bare `... = 2` with the default
            /// `max_parallel_replicas = 1` is a no-op everywhere else, so it must
            /// stay a no-op here too and just be disabled below (as mode `1`,
            /// best-effort, always is).
            ///
            /// Every parallel-replica algorithm is covered, not just the
            /// task-based one: custom-key (`canUseParallelReplicasCustomKey`) and
            /// sampling/offset (`canUseOffsetParallelReplicas`) modes are not
            /// gated by `canUseTaskBasedParallelReplicas`, so checking only the
            /// latter would let a forced custom-key/sampling run be silently
            /// downgraded here, breaking the force-or-throw contract. The disable
            /// below (setting the mode to `0`) does turn off all of them, since
            /// every `canUse*` predicate requires the setting to be `> 0`.
            ///
            /// The settings alone are not enough, though: the part of the recursive query
            /// governed by this very context also has to contain a table parallel replicas
            /// could be engaged for (see `context_may_engage_parallel_replicas`). A
            /// self-referential recursion that reads only the in-memory working table would
            /// never use parallel replicas in the planner either, so rejecting it merely
            /// because the profile enables the forcing mode would be a backwards-incompatible
            /// change unrelated to the stale `GLOBAL JOIN` cache hazard guarded here. The
            /// property is per context rather than per query, so that a branch-local
            /// `SETTINGS` clause is not rejected because of a sibling branch. The
            /// engagement walk also mirrors the planner's *silent* join-shape disable
            /// (`should_disable_parallel_replicas` in `PlannerJoinTree.cpp`, e.g. a
            /// `RIGHT` join with a remote right side): a join tree the planner would
            /// disable parallel replicas for anyway — even under the forcing mode —
            /// contributes no engagement, so such a step runs plainly instead of being
            /// rejected (see `plannerDisablesParallelReplicasForJoinTreeShape`).
            auto may_engage_it = context_may_engage_parallel_replicas.find(ctx.get());
            const ParallelReplicasEngagement engagement = may_engage_it != context_may_engage_parallel_replicas.end()
                ? may_engage_it->second
                : ParallelReplicasEngagement{};

            /// Even under the forcing mode the planner itself does not always throw. When
            /// `parallel_replicas_min_number_of_rows_per_replica > 0`, a task-based read
            /// served from a local `MergeTree` table first estimates the rows to read and
            /// *silently disables* parallel replicas when the estimate is below the
            /// threshold (see `PlannerJoinTree`: "Disabling parallel replicas because
            /// there aren't enough rows to read") — even with `... = 2`. A small recursive
            /// step over such a table would therefore run plainly on the non-recursive
            /// path, and rejecting it here would fail a query the plain planner accepts.
            /// The estimate needs per-step index analysis that cannot run ahead of
            /// planning, so when it *could* still disable parallel replicas — the
            /// threshold is set, the mode is the task-based one (the only one the
            /// estimate applies to), and the candidate has exactly one local `MergeTree`
            /// read — or a `Merge` may prune its preflight count down to one with a
            /// `_table` / `_database` filter — (a `ClusterProxy`-served read never runs
            /// the estimate) — do not throw
            /// preemptively; fall through to the disable below, mirroring the planner's
            /// own silent disable. Outside a prunable `Merge`, the exact-one check still
            /// matters: multi-read shapes such as a join of two local `MergeTree` tables
            /// or a view over `UNION ALL` skip the estimate in `PlannerJoinTree` and must
            /// still fail closed in forcing mode.
            /// For a step the estimate would have let through this keeps parallel replicas
            /// off — the same documented under-throw trade-off as elsewhere in this file:
            /// the read stays correct.
            const bool row_estimate_may_disable_parallel_replicas
                = ctx->getSettingsRef()[Setting::parallel_replicas_min_number_of_rows_per_replica] > 0
                && ctx->canUseTaskBasedParallelReplicas()
                && !engagement.remote
                && (engagement.local_merge_tree_read_count == 1
                    || engagement.local_merge_tree_read_count_may_be_reduced_by_merge_pruning);

            if (ctx->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas] >= 2
                && engagement.any()
                && !row_estimate_may_disable_parallel_replicas
                && (ctx->canUseTaskBasedParallelReplicas()
                    || ctx->canUseParallelReplicasCustomKey()
                    || ctx->canUseOffsetParallelReplicas()))
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Parallel replicas (allow_experimental_parallel_reading_from_replicas = 2) are not supported for the "
                    "recursive part of a recursive CTE. Set it to 0 or 1 to run the query.");

            auto new_ctx = Context::createCopy(ctx);
            new_ctx->setSetting("allow_experimental_parallel_reading_from_replicas", Field(UInt64(0)));
            context_copies.emplace(ctx.get(), new_ctx);
            ctx = std::move(new_ctx);
        };

        std::vector<IQueryTreeNode *> nodes_to_visit;
        nodes_to_visit.push_back(recursive_query.get());
        while (!nodes_to_visit.empty())
        {
            auto * node = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * qn = node->as<QueryNode>())
                rewrite_context(qn->getMutableContext());
            else if (auto * un = node->as<UnionNode>())
                rewrite_context(un->getMutableContext());

            for (auto & child : node->getChildren())
                if (child)
                    nodes_to_visit.push_back(child.get());
        }

        recursive_query_context = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getMutableContext() :
            recursive_query->as<UnionNode &>().getMutableContext();

        /// Collect every distinct context of the recursive query tree (after the rewrite
        /// above, so on the legacy path the rewritten copies are the ones collected). The
        /// working table has to be (re-)registered as an external table in each of them
        /// before every step, not only in the root's context: the query-tree builder gives
        /// every `QueryNode`/`UnionNode` its own `Context` copy, and
        /// `Context::getExternalTables` overlays the node-local mapping over the *query*
        /// context — never over an intermediate parent — so an entry added to the root
        /// recursive context alone is invisible to a branch's own context (a union branch,
        /// or any subquery with a `SETTINGS` clause). Those branch contexts are exactly
        /// what plan-based parallel replicas ship to the remote replicas:
        /// `ReadFromParallelReplicasStep` sends `context->getExternalTables()` of the
        /// context that planned the fragment's read. Today a fragment can never contain
        /// the working-table read itself (`ReadFromMemoryStorage` is not serializable, so
        /// the join is not lifted into the fragment and runs on the initiator), but that
        /// is an accident of step serializability — keep every context's view of the
        /// working table current so a fragment that does reference it resolves the
        /// frontier of the current step, not a stale or missing one.
        {
            std::set<const Context *> seen_contexts;
            std::vector<IQueryTreeNode *> nodes_to_scan;
            nodes_to_scan.push_back(recursive_query.get());
            while (!nodes_to_scan.empty())
            {
                auto * node = nodes_to_scan.back();
                nodes_to_scan.pop_back();

                ContextMutablePtr node_context;
                if (auto * qn = node->as<QueryNode>())
                    node_context = qn->getMutableContext();
                else if (auto * un = node->as<UnionNode>())
                    node_context = un->getMutableContext();

                if (node_context && seen_contexts.emplace(node_context.get()).second)
                    recursive_query_tree_contexts.push_back(std::move(node_context));

                for (auto & child : node->getChildren())
                    if (child)
                        nodes_to_scan.push_back(child.get());
            }
        }

        /// The seed (non-recursive) query keeps its original context, which was
        /// not rewritten above and therefore still permits parallel replicas.
        /// The seed runs once, never references the working table, and does not
        /// reuse a cached GLOBAL JOIN, so the cache-collision hazard that forces
        /// us to disable parallel replicas for recursive steps does not apply to
        /// it. Running it with this context (instead of the recursive one) keeps
        /// the parallel-replicas disable scoped to recursive iterations only.
        non_recursive_query_context = non_recursive_query->as<QueryNode>() ? non_recursive_query->as<QueryNode &>().getMutableContext() :
            non_recursive_query->as<UnionNode &>().getMutableContext();

        const auto & recursive_query_projection_columns = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getProjectionColumns() :
            recursive_query->as<UnionNode &>().computeProjectionColumns();

        if (recursive_cte_table->columns.size() != recursive_query_projection_columns.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Recursive CTE subquery {}. Expected projection columns to have same size in recursive and non recursive subquery.",
            recursive_cte_union_node->formatASTForErrorMessage());

        working_temporary_table_holder = recursive_cte_table->holder;
        working_temporary_table_storage = recursive_cte_table->storage;

        intermediate_temporary_table_holder = std::make_shared<TemporaryTableHolder>(
            recursive_query_context,
            ColumnsDescription{NamesAndTypesList{recursive_cte_table->columns.begin(), recursive_cte_table->columns.end()}},
            ConstraintsDescription{},
            nullptr /*query*/,
            true /*create_for_global_subquery*/);
        intermediate_temporary_table_storage = intermediate_temporary_table_holder->getTable();

        /// Collect equi-join keys between the CTE table and physical tables.
        /// Filters built from working-table values will be ANDed into each
        /// containing `QueryNode`'s WHERE during the recursive step.
        cte_join_keys = collectCTEJoinKeys(*recursive_query, recursive_table_nodes);
        for (const auto & key : cte_join_keys)
        {
            /// Snapshot `WHERE`, `HAVING`, and `QUALIFY` together. The planner
            /// mutates all three in place when building the pipeline: it can
            /// merge `QUALIFY` into `HAVING` (no window functions) and `HAVING`
            /// into `WHERE` (no aggregation), clearing the source clause in
            /// each case (see `Planner::buildPlanForQueryNode`). Restoring only
            /// `WHERE` between steps would drop those merged predicates on
            /// step 3+, because on step 2 they get moved into the WHERE we
            /// then overwrite with the snapshot.
            auto * qn = key.containing_query_node;
            original_clauses.emplace(qn, OriginalClauses{qn->getWhere(), qn->getHaving(), qn->getQualify()});
        }
    }

    Chunk generate()
    {
        Chunk current_chunk;

        while (!finished)
        {
            if (!executor.has_value())
                buildStepExecutor();

            while (current_chunk.getNumRows() == 0 && executor->pull(current_chunk))
            {
            }

            read_rows_during_recursive_step += current_chunk.getNumRows();

            if (current_chunk.getNumRows() > 0)
                break;

            executor.reset();

            if (read_rows_during_recursive_step == 0)
            {
                finished = true;
                truncateTemporaryTable(intermediate_temporary_table_storage);
                continue;
            }

            read_rows_during_recursive_step = 0;

            for (auto & recursive_table_node : recursive_table_nodes)
                recursive_table_node->updateStorage(intermediate_temporary_table_storage, recursive_query_context);

            truncateTemporaryTable(working_temporary_table_storage);

            std::swap(intermediate_temporary_table_holder, working_temporary_table_holder);
            std::swap(intermediate_temporary_table_storage, working_temporary_table_storage);
        }

        return current_chunk;
    }

private:
    /// Inject `WHERE original_where AND col IN (values)` into each affected
    /// `QueryNode` before executing a recursive step. Each step rebuilds the
    /// filter from the pristine original WHERE saved at construction time, so
    /// nothing accumulates across steps.
    ///
    /// Returns true if at least one filter was injected. Each key is handled
    /// independently: if for some key the join-key cardinality exceeded the
    /// configured cap, the generated `IN` set would exceed the user's
    /// `max_rows_in_set` / `max_bytes_in_set` limits (or fail to materialize
    /// under `max_memory_usage`), or the generated `IN` predicate could not be
    /// resolved for the join-key type, that single key is skipped while the safe
    /// predicates already collected for the other keys/branches are still
    /// installed. Because every generated predicate is independently
    /// semantics-preserving, dropping one never affects the correctness (or the
    /// pruning) of the others — a mixed recursive query keeps primary-key pruning
    /// on its safe `MergeTree` branches even when an unrelated branch has to fall
    /// back. If no key could be injected the caller runs the step unfiltered
    /// (the caller restores original clauses).
    bool injectFiltersIntoRecursiveQuery()
    {
        if (cte_join_keys.empty())
            return false;

        /// Group join keys by their containing `QueryNode`. A `QueryNode` may
        /// have multiple joins against the CTE — their predicates are combined
        /// with `AND`.
        std::map<QueryNode *, std::vector<QueryTreeNodePtr>> predicates_by_query;

        for (const auto & key : cte_join_keys)
        {
            /// The injected `IN` set is lowered by the planner using the
            /// settings of the `QueryNode` that contains the join, not the
            /// outer recursive context. For a recursive CTE with more than two
            /// branches the recursive part is a synthetic `UnionNode` whose
            /// context can differ from an individual branch's (e.g. a branch
            /// carrying `SETTINGS max_rows_in_set = 1`, or disabling the
            /// optimization via `recursive_cte_max_in_filter_cardinality = 0`).
            /// So the cardinality cap, the set-limit guard, and the filter
            /// construction must all use the containing query's own context to
            /// match what the planner will later see for that branch.
            const auto containing_query_context = key.containing_query_node->getContext();

            const auto & containing_settings = containing_query_context->getSettingsRef();
            const UInt64 max_in_filter_cardinality = containing_settings[Setting::recursive_cte_max_in_filter_cardinality];
            const UInt64 max_bytes_in_set = containing_settings[Setting::max_bytes_in_set];

            /// The optimization is disabled for this branch — skip its filter.
            /// Other branches keep theirs: each generated predicate is
            /// independently semantics-preserving.
            if (max_in_filter_cardinality == 0)
                continue;

            /// The generated `IN` matches keys on their raw representation,
            /// exactly like the hash-family join algorithms: `-0. IN (0.)` is
            /// `0` and NaNs with different payloads do not match. The
            /// sort/merge-based algorithms (`full_sorting_merge`,
            /// `parallel_full_sorting_merge`, `partial_merge`,
            /// `prefer_partial_merge`, and `auto`, which may
            /// fall back to `partial_merge`) instead compare floating-point
            /// keys by value through `compareAt`, where `+0.` equals `-0.`
            /// and all NaNs are equal. Under such an algorithm the prefilter
            /// could drop a table row (e.g. keyed `-0.`) that the join itself
            /// (probed with a `+0.` frontier value) would still match —
            /// losing a recursion branch. Fail closed: skip injection for a
            /// floating-point join key unless every join algorithm the
            /// branch may use matches keys on the raw representation, the way
            /// the `IN` does. Integer/string/etc. keys compare identically
            /// under both schemes and stay optimized for all algorithms.
            if ((typeInvolvesFloatingPoint(key.cte_column_type) || typeInvolvesFloatingPoint(key.real_column_node->getColumnType()))
                && joinAlgorithmsMayCompareFloatsByValue(containing_settings[Setting::join_algorithm]))
                continue;

            /// Reading the frontier values and materializing the RHS tuple must
            /// themselves fail closed. A tight `max_memory_usage` can make either
            /// step raise `MEMORY_LIMIT_EXCEEDED` (the memory tracker fires on the
            /// allocations); the unoptimized recursive scan never builds these, so
            /// such a failure must skip injection for this key and let it fall back
            /// to a plain scan, not fail the whole recursive query — exactly like
            /// the probe-set build inside `generatedInSetIsSafeToInject`. The
            /// cheaper limit breaches (cardinality / byte budget) are reported as
            /// nullopt rather than thrown. Skipping is per key: the other keys keep
            /// their safe predicates.
            std::optional<std::vector<Field>> values;
            std::shared_ptr<ConstantNode> rhs_node;
            try
            {
                values = readColumnValuesFromMemoryStorage(
                    working_temporary_table_storage, key.cte_column_name, recursive_query_context,
                    max_in_filter_cardinality, max_bytes_in_set);

                if (!values.has_value())
                    continue;

                if (values->empty())
                    continue;

                rhs_node = buildInRhsConstantNode(key.cte_column_type, *values);
            }
            catch (...) // Ok: building the generated IN values hit a limit (e.g. memory); skip injection for this key and fall back to a plain scan for it instead of failing the recursive query.
            {
                continue;
            }

            /// Fail closed if the planner could not build the generated `IN`
            /// set without changing the query's behaviour: the set could exceed
            /// the user's `max_rows_in_set` / `max_bytes_in_set` limits (throwing
            /// `SET_SIZE_LIMIT_EXCEEDED` or silently truncating), or the
            /// conversion to the storage column type could throw (e.g.
            /// `UNKNOWN_ELEMENT_OF_ENUM`). Neither can happen on the unoptimized
            /// scan path, so in both cases we skip injection for this key (the
            /// other keys keep their safe predicates).
            if (!generatedInSetIsSafeToInject(key.real_column_node->getColumnType(), rhs_node, containing_query_context))
                continue;

            /// Building the predicate resolves the `in` function's return type,
            /// which can itself throw for a join key whose type is valid for
            /// `JOIN` but rejected by `IN` — e.g. a `Dynamic` key allowed by
            /// `allow_dynamic_type_in_join_keys` that `FunctionIn::getReturnTypeImpl`
            /// refuses. The unoptimized scan never builds this predicate, so a
            /// resolution failure must skip injection for this key and let it fall
            /// back to a plain scan rather than fail the recursive query — exactly
            /// like the conversion and set-build guards above (the other keys keep
            /// their safe predicates).
            QueryTreeNodePtr in_filter;
            try
            {
                in_filter = buildInFilterNode(*key.real_column_node, std::move(rhs_node), containing_query_context);
            }
            catch (...) // Ok: resolving the generated IN predicate failed (e.g. a type illegal for IN); skip injection for this key and fall back to a plain scan for it instead of failing the recursive query.
            {
                continue;
            }

            predicates_by_query[key.containing_query_node].push_back(std::move(in_filter));
        }

        bool injected_any = false;
        for (auto & [query_node, predicates] : predicates_by_query)
        {
            if (predicates.empty())
                continue;

            auto cte_filter = conjoinPredicates(std::move(predicates), recursive_query_context);

            const auto & original_where = original_clauses.at(query_node).where;
            if (original_where)
                query_node->getWhere() = conjoinPredicates({original_where, std::move(cte_filter)}, recursive_query_context);
            else
                query_node->getWhere() = std::move(cte_filter);

            injected_any = true;
        }

        return injected_any;
    }

    void restoreOriginalClauses()
    {
        for (auto & [query_node, original] : original_clauses)
        {
            query_node->getWhere() = original.where;
            query_node->getHaving() = original.having;
            query_node->getQualify() = original.qualify;
        }
    }

    void buildStepExecutor()
    {
        const auto & recursive_subquery_settings = recursive_query_context->getSettingsRef();

        if (recursive_step > recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth])
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum recursive CTE evaluation depth ({}) exceeded, during evaluation of {}. Consider raising "
                "max_recursive_cte_evaluation_depth setting.",
                recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth].value,
                recursive_cte_union_node->formatASTForErrorMessage());

        auto & query_to_execute = recursive_step > 0 ? recursive_query : non_recursive_query;
        ++recursive_step;

        SelectQueryOptions select_query_options;

        const auto & recursive_table_name = recursive_cte_union_node->as<UnionNode &>().getCTEName();
        for (const auto & tree_context : recursive_query_tree_contexts)
            tree_context->addOrUpdateExternalTable(recursive_table_name, working_temporary_table_holder);

        /// `recursive_step` was already incremented above, so the seed query is
        /// step 1 and recursive iterations are step >1. Run the seed with its
        /// own context (parallel replicas enabled) and the recursive steps with
        /// the rewritten context (parallel replicas disabled).
        const auto & interpreter_context = recursive_step > 1 ? recursive_query_context : non_recursive_query_context;

        /// recursive_step was already incremented above — `>1` means we are
        /// executing the recursive query (the seed query is step `1`).
        const bool filters_injected = recursive_step > 1 && injectFiltersIntoRecursiveQuery();

        try
        {
            buildAndCapturePipeline(query_to_execute, interpreter_context, select_query_options);
        }
        catch (...)
        {
            /// The injected `IN` filters are only an optimization and must never
            /// turn a query the unoptimized scan would have run into a failure.
            /// `generatedInSetIsSafeToInject` builds a probe set to reject filters
            /// that would not fit, but it runs before `buildQueryPipeline`, and the
            /// real `FutureSetFromTuple` set is materialized here during pipeline
            /// construction, after further planner allocations have already been
            /// charged to the same memory tracker. A query sitting near
            /// `max_memory_usage` can therefore pass the probe and still raise
            /// `MEMORY_LIMIT_EXCEEDED` when the real set is built. Fail closed:
            /// restore the pristine clauses and retry the step once as a plain
            /// scan — exactly the pipeline the optimization-disabled path would
            /// have run. If the plain scan also fails, that failure is genuine
            /// (the query would have failed anyway) and propagates.
            if (!filters_injected)
            {
                restoreOriginalClauses();
                throw;
            }

            restoreOriginalClauses();
            buildAndCapturePipeline(query_to_execute, interpreter_context, select_query_options);
        }

        /// The pipeline was built and captured the (filter-injected) state of
        /// the query tree. The tree itself is reused across steps, so restore
        /// the original clauses now to leave it pristine for the next step.
        restoreOriginalClauses();
    }

    /// Build the pipeline for `query_to_execute` and capture it into `pipeline` /
    /// `executor`. Factored out of `buildStepExecutor` so the recursive step can
    /// be retried once as a plain scan (with the injected filters removed) when
    /// the first, filter-injected build fails — see the caller.
    void buildAndCapturePipeline(
        const QueryTreeNodePtr & query_to_execute,
        const ContextMutablePtr & interpreter_context,
        const SelectQueryOptions & select_query_options)
    {
        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_to_execute, interpreter_context, select_query_options);
        auto pipeline_builder = interpreter->buildQueryPipeline();

        pipeline_builder.addSimpleTransform([&](const SharedHeader & in_header)
        {
            return std::make_shared<MaterializingTransform>(in_header);
        });

        auto convert_to_temporary_tables_header_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline_builder.getHeader().getColumnsWithTypeAndName(),
            header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            interpreter->getContext());
        auto convert_to_temporary_tables_header_actions = std::make_shared<ExpressionActions>(std::move(convert_to_temporary_tables_header_actions_dag));
        pipeline_builder.addSimpleTransform([&](const SharedHeader & input_header)
        {
            return std::make_shared<ExpressionTransform>(input_header, convert_to_temporary_tables_header_actions);
        });

        /// TODO: Support squashing transform

        const auto metadata_snapshot = intermediate_temporary_table_storage->getInMemoryMetadataPtr(recursive_query_context, false);
        auto intermediate_temporary_table_storage_sink = intermediate_temporary_table_storage->write(
            {},
            metadata_snapshot,
            recursive_query_context,
            false /*async_insert*/);

        pipeline_builder.addChain(Chain(std::move(intermediate_temporary_table_storage_sink)));

        pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        pipeline.setProgressCallback(recursive_query_context->getProgressCallback());
        pipeline.setProcessListElement(recursive_query_context->getProcessListElement());

        executor.emplace(pipeline);
    }

    void truncateTemporaryTable(StoragePtr & temporary_table)
    {
        /// TODO: Support proper locking
        TableExclusiveLockHolder table_exclusive_lock;
        const auto metadata_snapshot = temporary_table->getInMemoryMetadataPtr(recursive_query_context, false);
        temporary_table->truncate({},
            metadata_snapshot,
            recursive_query_context,
            table_exclusive_lock);
    }

    SharedHeader header;
    QueryTreeNodePtr recursive_cte_union_node;
    std::vector<TableNode *> recursive_table_nodes;

    QueryTreeNodePtr non_recursive_query;
    QueryTreeNodePtr recursive_query;
    ContextMutablePtr recursive_query_context;
    ContextMutablePtr non_recursive_query_context;
    /// Every distinct context of the recursive query tree; the working table is registered
    /// as an external table in all of them before each step (see the constructor).
    std::vector<ContextMutablePtr> recursive_query_tree_contexts;

    TemporaryTableHolderPtr working_temporary_table_holder;
    StoragePtr working_temporary_table_storage;

    TemporaryTableHolderPtr intermediate_temporary_table_holder;
    StoragePtr intermediate_temporary_table_storage;

    QueryPipeline pipeline;
    std::optional<PullingAsyncPipelineExecutor> executor;

    std::vector<CTEJoinKey> cte_join_keys;
    /// Pristine `WHERE`, `HAVING`, and `QUALIFY` clauses captured at
    /// construction time, one entry per affected `QueryNode`. The recursive
    /// step rebuilds `WHERE = original_where AND in(...)` from these, then
    /// restores all three after the pipeline has been built — the planner
    /// folds `QUALIFY` into `HAVING` and `HAVING` into `WHERE` in place, and
    /// not restoring `HAVING`/`QUALIFY` would lose those clauses on step 3+.
    struct OriginalClauses
    {
        QueryTreeNodePtr where;
        QueryTreeNodePtr having;
        QueryTreeNodePtr qualify;
    };
    std::map<QueryNode *, OriginalClauses> original_clauses;

    size_t recursive_step = 0;
    size_t read_rows_during_recursive_step = 0;
    bool finished = false;
};

RecursiveCTESource::RecursiveCTESource(SharedHeader header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISource(header)
    , generator(std::make_unique<RecursiveCTEChunkGenerator>(std::move(header), std::move(recursive_cte_union_node_)))
{}

RecursiveCTESource::~RecursiveCTESource() = default;

Chunk RecursiveCTESource::generate()
{
    return generator->generate();
}

}
