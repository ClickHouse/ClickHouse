#include <array>
#include <memory>
#include <optional>
#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>

#include <base/sleep.h>
#include <Common/checkStackSize.h>
#include <Common/FailPoint.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ConvertingActions.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromParallelReplicas.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
}

namespace FailPoints
{
    extern const char slowdown_parallel_replicas_local_plan_read[];
}

/// Finds and returns the first QueryPlan node containing the specified ReadingStep type or nullptr
template <class ReadingStep>
static QueryPlan::Node * findReadingStep(QueryPlan::Node * node)
{
    ReadingStep * reading_step = nullptr;
    while (node)
    {
        reading_step = typeid_cast<ReadingStep *>(node->step.get());
        if (reading_step)
            break;

        if (!node->children.empty())
        {
            // in case of RIGHT JOIN, - reading from right table is parallelized among replicas
            const JoinStep * join = typeid_cast<JoinStep *>(node->step.get());
            const JoinStepLogical * join_logical = typeid_cast<JoinStepLogical *>(node->step.get());
            if ((join && join->getJoin()->getTableJoin().kind() == JoinKind::Right)
                || (join_logical && join_logical->getJoinOperator().kind == JoinKind::Right))
                node = node->children.at(1);
            else
                node = node->children.at(0);
        }
        else
            node = nullptr;
    }

    return node;
}

/// Walk the plan using the same traversal as findReadingStep (following LEFT/RIGHT JOIN logic),
/// but look for a UnionStep. If found, collect all ReadFromMergeTree steps from each child branch,
/// recursively handling nested views with their own UNION ALL.
std::vector<QueryPlan::Node *> findReadingSteps(QueryPlan::Node * root, bool allow_view_over_mergetree)
{
    auto * node = root;
    while (node)
    {
        if (typeid_cast<const ReadFromMergeTree *>(node->step.get()))
        {
            /// Single reading step, not under a union — return it as a single-element vector.
            return {node};
        }

        /// A UnionStep that is NOT the plan root comes from a view expansion (e.g. UNION ALL view).
        /// If it IS the root, it's the outer query's UNION and should not be treated as a view.
        /// Only consider union steps when parallel_replicas_allow_view_over_mergetree is enabled.
        if (allow_view_over_mergetree && node != root && typeid_cast<UnionStep *>(node->step.get()))
        {
            /// Found a UnionStep from a view — recursively collect ReadFromMergeTree from each
            /// child branch. This handles nested views whose inner queries also contain UNION ALL.
            std::vector<QueryPlan::Node *> result;
            for (auto * child : node->children)
            {
                auto child_results = findReadingSteps(child, allow_view_over_mergetree);
                result.insert(result.end(), child_results.begin(), child_results.end());
            }
            return result;
        }

        if (!node->children.empty())
        {
            const JoinStep * join = typeid_cast<JoinStep *>(node->step.get());
            const JoinStepLogical * join_logical = typeid_cast<JoinStepLogical *>(node->step.get());
            if ((join && join->getJoin()->getTableJoin().kind() == JoinKind::Right)
                || (join_logical && join_logical->getJoinOperator().kind == JoinKind::Right))
                node = node->children.at(1);
            else
                node = node->children.at(0);
        }
        else
            node = nullptr;
    }

    return {};
}

std::shared_ptr<const QueryPlan> createRemotePlanForParallelReplicas(
    const QueryTreeNodePtr & query_tree,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage)
{
    checkStackSize();

    auto new_context = Context::createCopy(context);

    auto select_query_options = SelectQueryOptions(processed_stage);
    select_query_options.build_logical_plan = true;

    /// Positional arguments in the outer query were already resolved by the initiator.
    /// Use a context flag instead of disabling enable_positional_arguments so that
    /// view-inner queries on this node are still processed correctly.
    /// See https://github.com/ClickHouse/ClickHouse/issues/62289.
    new_context->setPositionalArgumentsAlreadyResolved(true);
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));

    /// Disable parallel replicas in every nested QueryNode/UnionNode context — otherwise
    /// nested subqueries would re-enter parallel-replicas execution. Mirrors `createLocalPlanForParallelReplicas`.
    auto remote_query_tree = query_tree->clone();
    {
        std::vector<IQueryTreeNode *> nodes_to_visit;
        nodes_to_visit.push_back(remote_query_tree.get());
        while (!nodes_to_visit.empty())
        {
            auto * current = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * query_node = current->as<QueryNode>())
            {
                auto node_context = Context::createCopy(query_node->getContext());
                node_context->setPositionalArgumentsAlreadyResolved(true);
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                query_node->getMutableContext() = std::move(node_context);
            }
            else if (auto * union_node = current->as<UnionNode>())
            {
                auto node_context = Context::createCopy(union_node->getContext());
                node_context->setPositionalArgumentsAlreadyResolved(true);
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                union_node->getMutableContext() = std::move(node_context);
            }

            for (auto & child : current->getChildren())
            {
                if (child)
                    nodes_to_visit.push_back(child.get());
            }
        }
    }

    auto interpreter = InterpreterSelectQueryAnalyzer(remote_query_tree, new_context, select_query_options);
    auto query_plan = std::make_shared<QueryPlan>(std::move(interpreter).extractQueryPlan());
    addConvertingActions(*query_plan, header, context);

    // TODO: fix view with UNION case for enabled serialize_query_plan separately (use findReadingSteps() instead)
    auto * node = findReadingStep<ReadFromTableStep>(query_plan->getRootNode());
    if (node)
        typeid_cast<ReadFromTableStep*>(node->step.get())->useParallelReplicas() = true;

    return query_plan;
}

ContextPtr getShippedFragmentContext(const QueryTreeNodePtr & query_tree, ContextPtr fallback)
{
    if (const auto * query_node = query_tree->as<QueryNode>())
        return query_node->getContext();
    if (const auto * union_node = query_tree->as<UnionNode>())
        return union_node->getContext();
    return fallback;
}

/// The initiator-local fragment is the initiator's share of the very same fragment the remote replicas run, so
/// the settings that shape the read-in-order pipeline must be the ones the fragment is shipped with. The step is
/// built with the outer query context (`buildQueryPlanForParallelReplicas` passes `planner_context->getQueryContext()`,
/// which is the outer context even when the fragment is a subquery with its own `SETTINGS`), and that context is
/// also the one every runtime setting lookup in `ReadFromMergeTree` goes through. So a subquery-scoped
/// `read_in_order_use_virtual_row_per_block` would be honoured by the remote replicas, which re-plan the shipped
/// fragment under it, but not by the initiator-local fragment, which would keep evaluating the per-part
/// `PrefetchingConcat` guard under the outer value.
///
/// The outer context still has to be the base: it carries the parallel-replicas plumbing the reading step needs
/// (the cluster for parallel replicas, `max_parallel_replicas`, the client info identifying the local replica).
/// So copy over exactly the read-in-order settings that `ReadFromMergeTree` consults at pipeline-build time.
/// This mirrors the optimizer-side override in `optimizeTree` (`ReadFromLocalParallelReplicaStep`): if a new
/// setting starts gating the in-order read path from the step context, it must be added here too.
///
/// The stream-budget pair is on the list because the `ReadFromMergeTree` constructor re-derives the stream
/// budget from the supplied context: it clamps (or, with the asynchronous-read pool, re-expands)
/// `requested_num_streams` by `max_streams_for_merge_tree_reading` before `copyReadInOrderContractFrom` runs,
/// and `output_streams_limit` is re-computed the same way. The rebuilt step is handed the shipped fragment's
/// already-budgeted stream count, so re-clamping it by the *outer* value would make the initiator-local
/// rebuild disagree with the remote replicas on how many read streams the fragment has - and for an in-order
/// read the per-part split streams are a coordinator contract: only the snapshot replica may introduce
/// stream ids, so streams the initiator did not build are dropped as unknown.
///
/// `fragment_context` is taken per reading step, not once for the whole fragment: with
/// `parallel_replicas_allow_view_over_mergetree` a view can expand into a `UNION ALL` whose branches carry their
/// own `SETTINGS`, and the analyzer gives each branch its own `QueryNode` context. Each branch is planned by its
/// own `Planner` under that context (`buildPlannerContext` takes the node's context), so on a remote replica each
/// branch's `ReadFromMergeTree` looks these settings up in the branch context. Deriving one context from the
/// fragment root would flatten those branches on the initiator only. Optimizer-level gates are not affected:
/// a plan is optimized once, under the top-level context, on the initiator and on the replicas alike.
static ContextPtr makeShippedFragmentReadingContext(const ContextPtr & context, const ContextPtr & fragment_context)
{
    static constexpr std::array read_in_order_runtime_settings{
        "read_in_order_use_virtual_row",
        "read_in_order_use_virtual_row_per_block",
        "read_in_order_two_level_merge_threshold",
        "max_streams_for_merge_tree_reading",
        "allow_asynchronous_read_from_io_pool_for_merge_tree",
    };

    if (!fragment_context || fragment_context.get() == context.get())
        return context;

    const auto & settings = context->getSettingsRef();
    const auto & fragment_settings = fragment_context->getSettingsRef();

    ContextMutablePtr reading_context;
    for (const auto * name : read_in_order_runtime_settings)
    {
        auto fragment_value = fragment_settings.get(name);
        if (fragment_value == settings.get(name))
            continue;

        if (!reading_context)
            reading_context = Context::createCopy(context);
        reading_context->setSetting(name, fragment_value);
    }

    if (!reading_context)
        return context;
    return reading_context;
}

std::pair<QueryPlanPtr, bool> createLocalPlanForParallelReplicas(
    const QueryTreeNodePtr & query_tree,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr analyzed_read_from_merge_tree,
    size_t replica_number)
{
    checkStackSize();

    /// Since we're passing a pre-analyzed query tree (not AST), the interpreter won't run
    /// query tree passes anyway. We must NOT set ignoreASTOptimizations() here because it
    /// causes isASTLevelOptimizationAllowed() to return false in PlannerContext, which changes
    /// how constant node names are generated (using source expression instead of _CAST wrapper),
    /// leading to column name mismatches with the expected header.
    auto select_query_options = SelectQueryOptions(processed_stage);
    select_query_options.is_local_shard_plan
        = processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
    /// The local replica's plan is united into the parent pipeline in this process.
    select_query_options.is_local_plan_for_distributed_query = true;

    /// Positional arguments in the outer query were already resolved by the initiator.
    /// Use a context flag instead of disabling enable_positional_arguments so that
    /// view-inner queries on this node are still processed correctly.
    /// See https://github.com/ClickHouse/ClickHouse/issues/62289.
    auto new_context = Context::createCopy(context);
    new_context->setPositionalArgumentsAlreadyResolved(true);
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));

    /// Clone the query tree and disable parallel replicas in ALL QueryNode/UnionNode contexts.
    /// Each node gets a copy of its own context with parallel replicas disabled.
    /// This is necessary because the Planner extracts the context from each QueryNode,
    /// and the original query_tree has contexts with parallel replicas enabled.
    /// Without updating all nodes, nested subqueries (e.g. in JOINs) would still have
    /// parallel replicas enabled in their contexts, causing the Planner to create
    /// additional `ParallelReplicasReadingCoordinator` instances.
    auto local_query_tree = query_tree->clone();
    {
        std::vector<IQueryTreeNode *> nodes_to_visit;
        nodes_to_visit.push_back(local_query_tree.get());
        while (!nodes_to_visit.empty())
        {
            auto * current = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * query_node = current->as<QueryNode>())
            {
                auto node_context = Context::createCopy(query_node->getContext());
                node_context->setPositionalArgumentsAlreadyResolved(true);
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                query_node->getMutableContext() = std::move(node_context);
            }
            else if (auto * union_node = current->as<UnionNode>())
            {
                auto node_context = Context::createCopy(union_node->getContext());
                node_context->setPositionalArgumentsAlreadyResolved(true);
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                union_node->getMutableContext() = std::move(node_context);
            }

            for (auto & child : current->getChildren())
            {
                if (child)
                    nodes_to_visit.push_back(child.get());
            }
        }
    }

    auto interpreter = InterpreterSelectQueryAnalyzer(local_query_tree, new_context, select_query_options);
    auto query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());

    const bool allow_view_over_mergetree = context->getSettingsRef()[Setting::parallel_replicas_allow_view_over_mergetree];
    auto reading_nodes = findReadingSteps(query_plan->getRootNode(), allow_view_over_mergetree);
    if (reading_nodes.empty())
    {
        /// it can happen if merge tree table is empty — it'll be replaced with ReadFromPreparedSource
        return {std::move(query_plan), false};
    }

    /// Pin the snapshot replica to the initiator-local replica_num BEFORE any announcement
    /// is sent (either locally from here or from remote replicas over the network).
    coordinator->setSnapshotReplicaNum(replica_number);

    /// For the first reading step, reuse the pre-analyzed result if available.
    ReadFromMergeTree::AnalysisResultPtr analyzed_result_ptr;
    if (analyzed_read_from_merge_tree.get())
    {
        auto * analyzed_merge_tree = typeid_cast<ReadFromMergeTree *>(analyzed_read_from_merge_tree.get());
        if (analyzed_merge_tree)
            analyzed_result_ptr = analyzed_merge_tree->getAnalyzedResult();
    }

    for (auto * reading_node : reading_nodes)
    {
        auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());

        /// The step was planned under the context of the query node it belongs to, which is the branch context
        /// for a view expanded into `UNION ALL`, so it is the shipped scope of this particular read.
        auto reading_context = makeShippedFragmentReadingContext(context, reading->getContext());

        MergeTreeAllRangesCallback all_ranges_cb
            = [coordinator](InitialAllRangesAnnouncement announcement) -> std::optional<InitialAllRangesAnnouncementResponse>
        { return coordinator->handleInitialAllRangesAnnouncement(std::move(announcement)); };

        MergeTreeReadTaskCallback read_task_cb = [coordinator](ParallelReadRequest req) -> std::optional<ParallelReadResponse>
        {
            fiu_do_on(FailPoints::slowdown_parallel_replicas_local_plan_read,
            {
                sleepForMilliseconds(20);
            });
            return coordinator->handleRequest(std::move(req));
        };

        auto read_from_merge_tree_parallel_replicas = reading->createLocalParallelReplicasReadingStep(
            reading_context, analyzed_result_ptr, std::move(all_ranges_cb), std::move(read_task_cb), replica_number);
        reading_node->step = std::move(read_from_merge_tree_parallel_replicas);

        /// Only the first reading step can reuse the pre-analyzed result.
        analyzed_result_ptr = nullptr;
    }

    addConvertingActions(*query_plan, header, context);

    return {std::move(query_plan), true};
}

/// Collect every ReadFromMergeTree in a shipped fragment. Unlike findReadingSteps, this descends into a
/// UnionStep even when it is the fragment root: in a fragment a root UNION is a view expansion whose
/// branches must all be coordinated. This matches how the remote fragment marks its reads
/// (ConvertToDistributedVisitor::buildPlanFragment); otherwise a non-aggregating `SELECT * FROM view`
/// leaves later union branches as plain local reads whose rows are also returned by the remote fragment.
static void collectReadFromMergeTreeSteps(QueryPlan::Node * node, std::vector<QueryPlan::Node *> & result)
{
    if (!node)
        return;

    if (typeid_cast<ReadFromMergeTree *>(node->step.get()))
    {
        result.push_back(node);
        return;
    }

    for (auto * child : node->children)
        collectReadFromMergeTreeSteps(child, result);
}

QueryPlanPtr createLocalPlanFragmentForParallelReplicas(
    ContextPtr context, QueryPlanPtr plan_fragment, ParallelReplicasReadingCoordinatorPtr coordinator, size_t replica_number)
{
    std::vector<QueryPlan::Node *> reading_nodes;
    collectReadFromMergeTreeSteps(plan_fragment->getRootNode(), reading_nodes);
    if (reading_nodes.empty())
    {
        /// it can happen if merge tree table is empty — it'll be replaced with ReadFromPreparedSource
        return plan_fragment;
    }

    for (auto * reading_node : reading_nodes)
    {
        auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());

        /// Only the coordinated read (marked for parallel reading) is split across replicas. A JOIN's other side is
        /// left as a plain full local read (broadcast), matching how it is read on remote replicas.
        if (!reading->isParallelReadingFromReplicas())
            continue;

        MergeTreeAllRangesCallback all_ranges_cb = [coordinator](InitialAllRangesAnnouncement announcement) -> std::optional<InitialAllRangesAnnouncementResponse>
        { return coordinator->handleInitialAllRangesAnnouncement(std::move(announcement)); };

        MergeTreeReadTaskCallback read_task_cb = [coordinator](ParallelReadRequest req) -> std::optional<ParallelReadResponse>
        {
            fiu_do_on(FailPoints::slowdown_parallel_replicas_local_plan_read, { sleepForMilliseconds(20); });
            return coordinator->handleRequest(std::move(req));
        };

        auto read_from_merge_tree_parallel_replicas = reading->createLocalParallelReplicasReadingStep(
            context, nullptr, std::move(all_ranges_cb), std::move(read_task_cb), replica_number);
        reading_node->step = std::move(read_from_merge_tree_parallel_replicas);
    }

    auto query_plan = std::make_unique<QueryPlan>();
    auto read_from_local = std::make_unique<ReadFromLocalParallelReplicaStep>(std::move(plan_fragment), std::move(context));
    query_plan->addStep(std::move(read_from_local));

    return query_plan;
}

QueryPlanPtr createRemotePlanFragmentForParallelReplicas(
    ContextPtr context,
    QueryPlanPtr plan_fragment,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    const ClusterPtr & cluster,
    const std::vector<ConnectionPoolPtr> & connection_pools,
    std::optional<size_t> exclude_pool_index)
{
    auto read_from_remote = std::make_unique<ReadFromParallelReplicasStep>(
        std::move(plan_fragment), cluster, coordinator, context, connection_pools, exclude_pool_index, cluster->getShardsInfo().at(0).pool);

    auto query_plan = std::make_unique<QueryPlan>();
    query_plan->addStep(std::move(read_from_remote));
    return query_plan;
}
}
