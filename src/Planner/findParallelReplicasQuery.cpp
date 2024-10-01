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
#include <Parsers/queryToString.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/Utils.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/buildQueryTreeForShard.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool parallel_replicas_allow_in_with_subquery;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

/// Returns a list of (sub)queries (candidates) which may support parallel replicas.
/// The rule is :
/// subquery has only LEFT or ALL INNER JOIN (or none), and left part is MergeTree table or subquery candidate as well.
///
/// Additional checks are required, so we return many candidates. The innermost subquery is on top.
std::stack<const QueryNode *> getSupportingParallelReplicasQuery(const IQueryTreeNode * query_tree_node)
{
    std::stack<const QueryNode *> res;

    while (query_tree_node)
    {
        auto join_tree_node_type = query_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
            {
                const auto & table_node = query_tree_node->as<TableNode &>();
                const auto & storage = table_node.getStorage();
                /// Here we check StorageDummy as well, to support a query tree with replaced storages.
                if (std::dynamic_pointer_cast<MergeTreeData>(storage) || typeid_cast<const StorageDummy *>(storage.get()))
                {
                    /// parallel replicas is not supported with FINAL
                    if (table_node.getTableExpressionModifiers() && table_node.getTableExpressionModifiers()->hasFinal())
                        return {};

                    return res;
                }

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
                res.push(&query_node_to_process);
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
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = query_tree_node->as<JoinNode &>();
                auto join_kind = join_node.getKind();
                auto join_strictness = join_node.getStrictness();

                bool can_parallelize_join =
                    join_kind == JoinKind::Left
                    || (join_kind == JoinKind::Inner && join_strictness == JoinStrictness::All);

                if (!can_parallelize_join)
                    return {};

                query_tree_node = join_node.getLeftTableExpression().get();
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
            auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();

            auto storage_dummy = std::make_shared<StorageDummy>(
                storage_snapshot->storage.getStorageID(),
                ColumnsDescription(storage_snapshot->getColumns(get_column_options)),
                storage_snapshot);

            auto dummy_table_node = std::make_shared<TableNode>(std::move(storage_dummy), getContext());

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

/// Find the best candidate for parallel replicas execution by verifying query plan.
/// If query plan has only Expression, Filter of Join steps, we can execute it fully remotely and check the next query.
/// Otherwise we can execute current query up to WithMergableStage only.
const QueryNode * findQueryForParallelReplicas(
    std::stack<const QueryNode *> stack,
    const std::unordered_map<const QueryNode *, const QueryPlan::Node *> & mapping,
    const Settings & settings)
{
    const QueryPlan::Node * prev_checked_node = nullptr;
    const QueryNode * res = nullptr;

    while (!stack.empty())
    {
        const QueryNode * subquery_node = stack.top();
        stack.pop();

        auto it = mapping.find(subquery_node);
        /// This should not happen ideally.
        if (it == mapping.end())
            break;

        const QueryPlan::Node * curr_node = it->second;
        const QueryPlan::Node * next_node_to_check = curr_node;
        bool can_distribute_full_node = true;

        while (next_node_to_check && next_node_to_check != prev_checked_node)
        {
            const auto & children = next_node_to_check->children;
            auto * step = next_node_to_check->step.get();

            if (children.empty())
            {
                /// Found a source step. This should be possible only in the first iteration.
                if (prev_checked_node)
                    return nullptr;

                next_node_to_check = nullptr;
            }
            else if (children.size() == 1)
            {
                const auto * expression = typeid_cast<ExpressionStep *>(step);
                const auto * filter = typeid_cast<FilterStep *>(step);

                const auto * creating_sets = typeid_cast<DelayedCreatingSetsStep *>(step);
                bool allowed_creating_sets = settings[Setting::parallel_replicas_allow_in_with_subquery] && creating_sets;

                if (!expression && !filter && !allowed_creating_sets)
                    can_distribute_full_node = false;

                next_node_to_check = children.front();
            }
            else
            {
                const auto * join = typeid_cast<JoinStep *>(step);
                /// We've checked that JOIN is INNER/LEFT in query tree.
                /// Don't distribute UNION node.
                if (!join)
                    return res;

                next_node_to_check = children.front();
            }
        }

        /// Current node contains steps like GROUP BY / DISTINCT
        /// Will try to execute query up to WithMergableStage
        if (!can_distribute_full_node)
        {
            /// Current query node does not contain subqueries.
            /// We can execute parallel replicas over storage::read.
            if (!res)
                return nullptr;

            return subquery_node;
        }

        /// Query is simple enough to be fully distributed.
        res = subquery_node;
        prev_checked_node = curr_node;
    }

    return res;
}

const QueryNode * findQueryForParallelReplicas(const QueryTreeNodePtr & query_tree_node, SelectQueryOptions & select_query_options)
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

    auto stack = getSupportingParallelReplicasQuery(query_tree_node.get());
    /// Empty stack means that storage does not support parallel replicas.
    if (stack.empty())
        return nullptr;

    /// We don't have any subquery and storage can process parallel replicas by itself.
    if (stack.top() == query_tree_node.get())
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
    auto new_stack = getSupportingParallelReplicasQuery(updated_query_tree.get());
    const auto & mapping = planner.getQueryNodeToPlanStepMapping();
    const auto * res = findQueryForParallelReplicas(new_stack, mapping, context->getSettingsRef());

    /// Now, return a query from initial stack.
    if (res)
    {
        while (!new_stack.empty())
        {
            if (res == new_stack.top())
                return stack.top();

            stack.pop();
            new_stack.pop();
        }
    }

    return res;
}

static const TableNode * findTableForParallelReplicas(const IQueryTreeNode * query_tree_node)
{
    std::stack<const IQueryTreeNode *> right_join_nodes;
    while (query_tree_node || !right_join_nodes.empty())
    {
        if (!query_tree_node)
        {
            query_tree_node = right_join_nodes.top();
            right_join_nodes.pop();
        }

        auto join_tree_node_type = query_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
            {
                const auto & table_node = query_tree_node->as<TableNode &>();
                const auto * as_mat_view = typeid_cast<const StorageMaterializedView *>(table_node.getStorage().get());
                const auto & storage = as_mat_view ? as_mat_view->getTargetTable() : table_node.getStorage();
                if (std::dynamic_pointer_cast<MergeTreeData>(storage) || typeid_cast<const StorageDummy *>(storage.get()))
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
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = query_tree_node->as<JoinNode &>();
                query_tree_node = join_node.getLeftTableExpression().get();
                right_join_nodes.push(join_node.getRightTableExpression().get());
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

const TableNode * findTableForParallelReplicas(const QueryTreeNodePtr & query_tree_node, SelectQueryOptions & select_query_options)
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

    return findTableForParallelReplicas(query_tree_node.get());
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
    modified_query_tree = buildQueryTreeForShard(planner_context, modified_query_tree);
    ASTPtr modified_query_ast = queryNodeToDistributedSelectQuery(modified_query_tree);

    Block header = InterpreterSelectQueryAnalyzer::getSampleBlock(
        modified_query_tree, context, SelectQueryOptions(processed_stage).analyze());

    const TableNode * table_node = findTableForParallelReplicas(modified_query_tree.get());
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
        storage_limits);

    auto converting = ActionsDAG::makeConvertingActions(
        header.getColumnsWithTypeAndName(),
        initial_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position);

    /// initial_header is a header expected by initial query.
    /// header is a header which is returned by the follower.
    /// They are different because tables will have different aliases (e.g. _table1 or _table5).
    /// Here we just rename columns by position, with the hope the types would match.
    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(converting));
    step->setStepDescription("Convert distributed names");
    query_plan.addStep(std::move(step));

    return {std::move(query_plan), std::move(processed_stage), {}, {}, {}};
}

}
