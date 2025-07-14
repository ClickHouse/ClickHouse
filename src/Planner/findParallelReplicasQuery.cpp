#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Core/Settings.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTSubquery.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/Utils.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/buildQueryTreeForShard.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool parallel_replicas_allow_in_with_subquery;
    extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

static bool canUseTableForParallelReplicas(const TableNode & table_node, const ContextPtr & context [[maybe_unused]])
{
    const auto & storage = table_node.getStorage();

    if (!storage->isMergeTree() && !typeid_cast<const StorageDummy *>(storage.get()))
        return false;

    if (!storage->supportsReplication() && !context->getSettingsRef()[Setting::parallel_replicas_for_non_replicated_merge_tree])
        return false;

    /// Parallel replicas not supported with FINAL.
    if (table_node.hasTableExpressionModifiers() && table_node.getTableExpressionModifiers()->hasFinal())
        return false;

    return true;
}

/// Returns a list of (sub)queries (candidates) which may support parallel replicas.
/// The rule is :
/// subquery has only LEFT / RIGHT / ALL INNER JOIN (or none), and left / right part is MergeTree table or subquery candidate as well.
///
/// Additional checks are required, so we return many candidates. The innermost subquery is on top.
std::vector<const QueryNode *> getSupportingParallelReplicasQuery(const IQueryTreeNode * query_tree_node, const ContextPtr & context)
{
    std::vector<const QueryNode *> res;

    while (query_tree_node)
    {
        auto join_tree_node_type = query_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
            {
                const auto & table_node = query_tree_node->as<TableNode &>();
                if (canUseTableForParallelReplicas(table_node, context))
                    return res;

                return {};
            }
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                return {};
            }
            case QueryTreeNodeType::QUERY:
            {
                const auto & query_node_to_process = query_tree_node->as<QueryNode &>();
                query_tree_node = query_node_to_process.getJoinTree().get();
                res.push_back(&query_node_to_process);
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                const auto & union_node = query_tree_node->as<UnionNode &>();
                const auto & union_queries = union_node.getQueries().getNodes();

                if (union_queries.empty())
                    return {};

                query_tree_node = union_queries.front().get();
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                const auto & array_join_node = query_tree_node->as<ArrayJoinNode &>();
                query_tree_node = array_join_node.getTableExpression().get();
                break;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                /// TODO: We can parallelize one table
                return {};
            }
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = query_tree_node->as<JoinNode &>();
                const auto join_kind = join_node.getKind();
                const auto join_strictness = join_node.getStrictness();

                if (join_kind == JoinKind::Left || (join_kind == JoinKind::Inner && join_strictness == JoinStrictness::All))
                    query_tree_node = join_node.getLeftTableExpression().get();
                else if (join_kind == JoinKind::Right && join_strictness != JoinStrictness::RightAny)
                    query_tree_node = join_node.getRightTableExpression().get();
                else
                    return {};

                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                query_tree_node->getNodeTypeName());
            }
        }
    }

    return res;
}

class ReplaceTableNodeToDummyVisitor : public InDepthQueryTreeVisitorWithContext<ReplaceTableNodeToDummyVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ReplaceTableNodeToDummyVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * table_node = node->as<TableNode>();
        auto * table_function_node = node->as<TableFunctionNode>();

        if (table_node || table_function_node)
        {
            const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            const auto & storage = storage_snapshot->storage;

            auto storage_dummy = std::make_shared<StorageDummy>(
                storage.getStorageID(),
                /// To preserve information about alias columns, column description must be extracted directly from storage metadata.
                storage_snapshot->metadata->getColumns(),
                storage_snapshot,
                storage.supportsReplication());

            auto dummy_table_node = std::make_shared<TableNode>(std::move(storage_dummy), getContext());
            if (table_node && table_node->hasTableExpressionModifiers())
                dummy_table_node->getTableExpressionModifiers() = table_node->getTableExpressionModifiers();

            dummy_table_node->setAlias(node->getAlias());
            replacement_map.emplace(node.get(), std::move(dummy_table_node));
        }
    }

    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> replacement_map;
};

QueryTreeNodePtr replaceTablesWithDummyTables(QueryTreeNodePtr query, const ContextPtr & context)
{
    ReplaceTableNodeToDummyVisitor visitor(context);
    visitor.visit(query);

    return query->cloneAndReplace(visitor.replacement_map);
}

#ifdef DUMP_PARALLEL_REPLICAS_QUERY_CANDIDATES
#include <ranges>

static void dumpStack(const std::vector<const QueryNode *> & stack)
{
    std::ranges::reverse_view rv{stack};
    for (const auto * node : rv)
        LOG_DEBUG(getLogger(__PRETTY_FUNCTION__), "{}\n{}", CityHash_v1_0_2::Hash128to64(node->getTreeHash()), node->dumpTree());
}
#endif

/// Find the best candidate for parallel replicas execution by verifying query plan.
/// If query plan has only Expression, Filter or Join steps, we can execute it fully remotely and check the next query.
/// Otherwise we can execute current query up to WithMergableStage only.
const QueryNode * findQueryForParallelReplicas(
    std::vector<const QueryNode *> stack,
    const std::unordered_map<const QueryNode *, const QueryPlan::Node *> & mapping,
    const Settings & settings)
{
#ifdef DUMP_PARALLEL_REPLICAS_QUERY_CANDIDATES
    dumpStack(stack);
#endif

    struct Frame
    {
        const QueryPlan::Node * node = nullptr;
        /// Below we will check subqueries from `stack` to find outermost subquery that could be executed remotely.
        /// Currently traversal algorithm considers only steps with 0 or 1 children and JOIN specifically.
        /// When we found some step that requires finalization on the initiator (e.g. GROUP BY) there are two options:
        /// 1. If plan looks like a single path (e.g. AggregatingStep -> ExpressionStep -> Reading) we can execute
        /// current subquery as a whole with replicas.
        /// 2. If we were inside JOIN we cannot offload the whole subquery to replicas because at least one side
        /// of the JOIN needs to be finalized on the initiator.
        /// So this flag is used to track what subquery to return once we hit a step that needs finalization.
        bool inside_join = false;
    };

    const QueryNode * res = nullptr;

    while (!stack.empty())
    {
        const QueryNode * const subquery_node = stack.back();
        stack.pop_back();

        auto it = mapping.find(subquery_node);
        /// This should not happen ideally.
        if (it == mapping.end())
            break;

        std::stack<Frame> nodes_to_check;
        nodes_to_check.push({.node = it->second, .inside_join = false});
        bool can_distribute_full_node = true;
        bool currently_inside_join = false;

        while (!nodes_to_check.empty())
        {
            /// Copy to avoid container overflow (we call pop() in the next line).
            const auto [next_node_to_check, inside_join] = nodes_to_check.top();
            nodes_to_check.pop();
            const auto & children = next_node_to_check->children;
            auto * step = next_node_to_check->step.get();

            if (children.empty())
            {
                /// Found a source step.
            }
            else if (children.size() == 1)
            {
                const auto * expression = typeid_cast<ExpressionStep *>(step);
                const auto * filter = typeid_cast<FilterStep *>(step);

                const auto * creating_sets = typeid_cast<DelayedCreatingSetsStep *>(step);
                const bool allowed_creating_sets = settings[Setting::parallel_replicas_allow_in_with_subquery] && creating_sets;

                const auto * sorting = typeid_cast<SortingStep *>(step);
                /// Sorting for merge join is supposed to be done locally before join itself, so it doesn't need finalization.
                const bool allowed_sorting = sorting && sorting->isSortingForMergeJoin();

                if (!expression && !filter && !allowed_creating_sets && !allowed_sorting)
                {
                    can_distribute_full_node = false;
                    currently_inside_join = inside_join;
                }

                nodes_to_check.push({.node = children.front(), .inside_join = inside_join});
            }
            else
            {
                const auto * join = typeid_cast<JoinStep *>(step);
                const auto * join_logical = typeid_cast<JoinStepLogical *>(step);
                if (join_logical && join_logical->hasPreparedJoinStorage())
                    /// JoinStepLogical with prepared storage is converted to FilledJoinStep, not regular JoinStep.
                    join_logical = nullptr;

                /// We've checked that JOIN is INNER/LEFT/RIGHT on query tree level before.
                /// Don't distribute UNION node.
                if (!join && !join_logical)
                    return res;

                for (const auto & child : children)
                    nodes_to_check.push({.node = child, .inside_join = true});
            }
        }

        if (!can_distribute_full_node)
        {
            /// Current query node does not contain subqueries.
            /// We can execute parallel replicas over storage::read.
            if (!res)
                return nullptr;

            return currently_inside_join ? res : subquery_node;
        }

        /// Query is simple enough to be fully distributed.
        res = subquery_node;
    }

    return res;
}

const QueryNode * findQueryForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options)
{
    if (select_query_options.only_analyze)
        return nullptr;

    auto * query_node = query_tree_node->as<QueryNode>();
    auto * union_node = query_tree_node->as<UnionNode>();

    if (!query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree_node->formatASTForErrorMessage());

    auto context = query_node ? query_node->getContext() : union_node->getContext();

    if (!context->canUseParallelReplicasOnInitiator())
        return nullptr;

    auto stack = getSupportingParallelReplicasQuery(query_tree_node.get(), context);
    /// Empty stack means that storage does not support parallel replicas.
    if (stack.empty())
        return nullptr;

    /// We don't have any subquery and storage can process parallel replicas by itself.
    if (stack.back() == query_tree_node.get())
        return nullptr;

    /// This is needed to avoid infinite recursion.
    auto mutable_context = Context::createCopy(context);
    mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));

    /// Here we replace tables to dummy, in order to build a temporary query plan for parallel replicas analysis.
    ResultReplacementMap replacement_map;
    auto updated_query_tree = replaceTablesWithDummyTables(query_tree_node, mutable_context);

    SelectQueryOptions options;
    Planner planner(updated_query_tree, options, std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));
    planner.buildQueryPlanIfNeeded();

    /// This part is a bit clumsy.
    /// We updated a query_tree with dummy storages, and mapping is using updated_query_tree now.
    /// But QueryNode result should be taken from initial query tree.
    /// So that we build a list of candidates again, and call findQueryForParallelReplicas for it.
    auto new_stack = getSupportingParallelReplicasQuery(updated_query_tree.get(), context);
    const auto & mapping = planner.getQueryNodeToPlanStepMapping();
    const auto * res = findQueryForParallelReplicas(new_stack, mapping, context->getSettingsRef());

    if (res)
    {
        // find query in initial stack
        while (!new_stack.empty())
        {
            if (res == new_stack.back())
            {
                res = stack.back();
                break;
            }

            stack.pop_back();
            new_stack.pop_back();
        }
    }
    return res;
}

static const TableNode * findTableForParallelReplicas(const IQueryTreeNode * query_tree_node, const ContextPtr & context)
{
    std::stack<const IQueryTreeNode *> join_nodes;
    while (query_tree_node || !join_nodes.empty())
    {
        if (!query_tree_node)
        {
            query_tree_node = join_nodes.top();
            join_nodes.pop();
        }

        auto join_tree_node_type = query_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
            {
                const auto & table_node = query_tree_node->as<TableNode &>();
                if (canUseTableForParallelReplicas(table_node, context))
                    return &table_node;

                query_tree_node = nullptr;
                break;
            }
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                query_tree_node = nullptr;
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                const auto & query_node_to_process = query_tree_node->as<QueryNode &>();
                query_tree_node = query_node_to_process.getJoinTree().get();
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                const auto & union_node = query_tree_node->as<UnionNode &>();
                const auto & union_queries = union_node.getQueries().getNodes();

                query_tree_node = nullptr;
                if (!union_queries.empty())
                    query_tree_node = union_queries.front().get();

                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                const auto & array_join_node = query_tree_node->as<ArrayJoinNode &>();
                query_tree_node = array_join_node.getTableExpression().get();
                break;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                /// TODO: We can parallelize one table
                return nullptr;
            }
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = query_tree_node->as<JoinNode &>();
                const auto join_kind = join_node.getKind();
                const auto join_strictness = join_node.getStrictness();

                if (join_kind == JoinKind::Left || (join_kind == JoinKind::Inner and join_strictness == JoinStrictness::All))
                {
                    query_tree_node = join_node.getLeftTableExpression().get();
                    join_nodes.push(join_node.getRightTableExpression().get());
                }
                else if (join_kind == JoinKind::Right)
                {
                    query_tree_node = join_node.getRightTableExpression().get();
                    join_nodes.push(join_node.getLeftTableExpression().get());
                }
                else
                {
                    return nullptr;
                }
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                query_tree_node->getNodeTypeName());
            }
        }
    }

    return nullptr;
}

const TableNode * findTableForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options)
{
    if (select_query_options.only_analyze)
        return nullptr;

    auto * query_node = query_tree_node->as<QueryNode>();
    auto * union_node = query_tree_node->as<UnionNode>();

    if (!query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree_node->formatASTForErrorMessage());

    auto context = query_node ? query_node->getContext() : union_node->getContext();

    if (!context->canUseParallelReplicasOnFollower())
        return nullptr;

    return findTableForParallelReplicas(query_tree_node.get(), context);
}

JoinTreeQueryPlan buildQueryPlanForParallelReplicas(
    const QueryNode & query_node,
    const PlannerContextPtr & planner_context,
    std::shared_ptr<const StorageLimitsList> storage_limits)
{
    auto processed_stage = QueryProcessingStage::WithMergeableState;
    auto context = planner_context->getQueryContext();

    QueryTreeNodePtr modified_query_tree = query_node.clone();

    Block initial_header = InterpreterSelectQueryAnalyzer::getSampleBlock(
        modified_query_tree, context, SelectQueryOptions(processed_stage).analyze());

    rewriteJoinToGlobalJoin(modified_query_tree, context);
    modified_query_tree = buildQueryTreeForShard(planner_context, modified_query_tree, /*allow_global_join_for_right_table*/ true);
    ASTPtr modified_query_ast = queryNodeToDistributedSelectQuery(modified_query_tree);

    Block header = InterpreterSelectQueryAnalyzer::getSampleBlock(
        modified_query_tree, context, SelectQueryOptions(processed_stage).analyze());

    const TableNode * table_node = findTableForParallelReplicas(modified_query_tree.get(), context);
    if (!table_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't determine table for parallel replicas");

    QueryPlan query_plan;
    ClusterProxy::executeQueryWithParallelReplicas(
        query_plan,
        table_node->getStorageID(),
        header,
        processed_stage,
        modified_query_ast,
        context,
        storage_limits,
        nullptr);

    auto converting = ActionsDAG::makeConvertingActions(
        header.getColumnsWithTypeAndName(),
        initial_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position);

    /// initial_header is a header expected by initial query.
    /// header is a header which is returned by the follower.
    /// They are different because tables will have different aliases (e.g. _table1 or _table5).
    /// Here we just rename columns by position, with the hope the types would match.
    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(converting));
    step->setStepDescription("Convert distributed names");
    query_plan.addStep(std::move(step));

    return {std::move(query_plan), std::move(processed_stage), {}, {}, {}};
}

}
