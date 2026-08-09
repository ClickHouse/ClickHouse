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
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/StorageMerge.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromParallelReplicas.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
    extern const SettingsBool parallel_replicas_allow_merge_tables;
}

namespace FailPoints
{
    extern const char slowdown_parallel_replicas_local_plan_read[];
}

/// Walk the plan following LEFT/RIGHT JOIN logic (only the side that parallel replicas coordinate),
/// but look for a UnionStep. If found, collect all ReadFromMergeTree steps from each child branch,
/// recursively handling nested views with their own UNION ALL.
std::vector<QueryPlan::Node *> findReadingSteps(QueryPlan::Node * root, bool allow_view_over_mergetree, bool allow_merge_tables)
{
    auto * node = root;
    while (node)
    {
        if (typeid_cast<const ReadFromMergeTree *>(node->step.get()))
        {
            /// Single reading step, not under a union — return it as a single-element vector.
            return {node};
        }

        /// Reading from a Merge table coordinates reading from every underlying MergeTree table
        /// by itself (see ReadFromMerge::enableParallelReplicasLocalPlan).
        if (allow_merge_tables && typeid_cast<const ReadFromMerge *>(node->step.get()))
        {
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
                auto child_results = findReadingSteps(child, allow_view_over_mergetree, allow_merge_tables);
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

/// Mark every reading step of a shipped logical fragment that the replicas must coordinate
/// (`ReadFromTableStep` / `ReadFromTableFunctionStep`, the latter for the merge(...) table
/// function - the only table function eligible for parallel replicas). The fragment can contain
/// several of them: a `UNION ALL` view expansion, or a `UNION ALL` over several `Merge` sources.
/// A read the replicas do not coordinate is planned by them as a plain local read, and every
/// replica would return its rows in full on top of the coordinated result of the marked reads,
/// duplicating them - so descend into every branch of a union. For a JOIN only the side that
/// parallel replicas coordinate is marked: the other side is a broadcast read that every replica
/// intentionally performs in full. Marking a read that the initiator's local plan does not
/// announce is safe: the snapshot replica is pinned to the initiator whenever a plan is shipped,
/// so the coordinator ignores announcements for streams the local plan does not know and assigns
/// such reads no work, while the initiator reads them in full by itself.
static void markReadsForParallelReplicas(QueryPlan::Node * node)
{
    while (node)
    {
        if (auto * read_from_table = typeid_cast<ReadFromTableStep *>(node->step.get()))
        {
            read_from_table->useParallelReplicas() = true;
            return;
        }

        if (auto * read_from_table_function = typeid_cast<ReadFromTableFunctionStep *>(node->step.get()))
        {
            read_from_table_function->useParallelReplicas() = true;
            return;
        }

        if (typeid_cast<UnionStep *>(node->step.get()))
        {
            for (auto * child : node->children)
                markReadsForParallelReplicas(child);
            return;
        }

        if (!node->children.empty())
        {
            /// In case of RIGHT JOIN, reading from the right table is parallelized among replicas.
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

    markReadsForParallelReplicas(query_plan->getRootNode());

    return query_plan;
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
    const bool allow_merge_tables = context->getSettingsRef()[Setting::parallel_replicas_allow_merge_tables];
    auto reading_nodes = findReadingSteps(query_plan->getRootNode(), allow_view_over_mergetree, allow_merge_tables);
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

        /// A Merge table creates its child reading steps lazily; it converts them into local
        /// parallel replicas reading steps itself (one data stream per underlying table).
        if (auto * reading_merge = typeid_cast<ReadFromMerge *>(reading_node->step.get()))
        {
            reading_merge->enableParallelReplicasLocalPlan(context, std::move(all_ranges_cb), std::move(read_task_cb), replica_number);
            continue;
        }

        auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());

        auto read_from_merge_tree_parallel_replicas = reading->createLocalParallelReplicasReadingStep(
            context, analyzed_result_ptr, std::move(all_ranges_cb), std::move(read_task_cb), replica_number);
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
