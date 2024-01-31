#include <Planner/findParallelReplicasQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Storages/buildQueryTreeForShard.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/Utils.h>
#include "Analyzer/ArrayJoinNode.h"
#include "Analyzer/InDepthQueryTreeVisitor.h"
#include "Analyzer/JoinNode.h"
#include "Analyzer/QueryNode.h"
#include "Analyzer/TableNode.h"
#include "Analyzer/UnionNode.h"
#include "Parsers/ASTSubquery.h"
#include "Parsers/queryToString.h"
#include "Processors/QueryPlan/ExpressionStep.h"
#include "Processors/QueryPlan/FilterStep.h"
#include "Storages/MergeTree/MergeTreeData.h"
#include <Storages/StorageDummy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

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
                if (std::dynamic_pointer_cast<MergeTreeData>(storage) || typeid_cast<const StorageDummy *>(storage.get()))
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

class ReplaceTableNodeToDummyVisitor : public InDepthQueryTreeVisitor<ReplaceTableNodeToDummyVisitor, true>
{
public:
    using Base = InDepthQueryTreeVisitor<ReplaceTableNodeToDummyVisitor, true>;
    using Base::Base;

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * table_node = node->as<TableNode>();
        auto * table_function_node = node->as<TableFunctionNode>();

        if (table_node || table_function_node)
        {
            const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();

            auto storage_dummy
                = std::make_shared<StorageDummy>(storage_snapshot->storage.getStorageID(), ColumnsDescription(storage_snapshot->getColumns(get_column_options)));

            auto dummy_table_node = std::make_shared<TableNode>(std::move(storage_dummy), context);

            dummy_table_node->setAlias(node->getAlias());
            replacement_map.emplace(node.get(), std::move(dummy_table_node));
        }
    }

    ContextPtr context;
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> replacement_map;
};

QueryTreeNodePtr replaceTablesWithDummyTables(const QueryTreeNodePtr & query, const ContextPtr & context)
{
    ReplaceTableNodeToDummyVisitor visitor;
    visitor.context = context;
    visitor.visit(query);

    return query->cloneAndReplace(visitor.replacement_map);
}

const QueryNode * findParallelReplicasQuery(
    std::stack<const QueryNode *> stack,
    const std::unordered_map<const QueryNode *, const QueryPlan::Node *> & mapping)
{
    const QueryPlan::Node * prev_checked_node = nullptr;
    const QueryNode * res = nullptr;

    while (!stack.empty())
    {
        const QueryNode * subquery_node = stack.top();
        stack.pop();

        // std::cerr << "----- trying " << reinterpret_cast<const void *>(subquery_node) << std::endl;

        // const QueryNode * mapped_node = subquery_node;
        // if (auto it = replacement_map.find(subquery_node); it != replacement_map.end())
        //     mapped_node = it->second.get();

        auto it = mapping.find(subquery_node);
        /// This should not happen ideally.
        if (it == mapping.end())
            break;

        const QueryPlan::Node * curr_node = it->second;
        const QueryPlan::Node * next_node_to_check = curr_node;
        bool can_distribute_full_node = true;

        // std::cerr << "trying " << curr_node->step->getName() << '\n' << subquery_node->dumpTree() << std::endl;

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
                if (!expression && !filter)
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
            /// Current query node does not contain subqueries already.
            /// We can execute parallel replicas over storage.
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

const QueryNode * findParallelReplicasQuery(const QueryTreeNodePtr & query_tree_node, SelectQueryOptions & select_query_options)
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
    // const auto & settings = context->getSettingsRef();

    if (!context->canUseParallelReplicasOnInitiator())
        return nullptr;

    auto stack = getSupportingParallelReplicasQuery(query_tree_node.get());
    // std::cerr << "=============== findParallelReplicasQuery stack size " << stack.size() << std::endl;
    // std::cerr << "=============== findParallelReplicasQuery tree\n " << query_tree_node->dumpTree() << std::endl;
    // std::cerr << "=============== findParallelReplicasQuery trace \n" << StackTrace().toString() << std::endl;
    /// Empty stack means that storage does not support parallel replicas.
    if (stack.empty())
        return nullptr;

    /// We don't have any subquery and storage can process parallel replicas by itself.
    if (stack.top() == query_tree_node.get())
        return nullptr;

    auto mutable_context = Context::createCopy(context);
    mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));

    ResultReplacementMap replacement_map;
    auto updated_query_tree = replaceTablesWithDummyTables(query_tree_node, mutable_context);

    // std::cerr << "=============== findParallelReplicasQuery updated tree\n " << updated_query_tree->dumpTree() << std::endl;

    SelectQueryOptions options;
    //options.only_analyze = true;
    Planner planner(updated_query_tree, options, std::make_shared<GlobalPlannerContext>(nullptr, nullptr));
    planner.buildQueryPlanIfNeeded();

    // WriteBufferFromOwnString buf;
    // planner.getQueryPlan().explainPlan(buf, {.actions = true});
    // std::cerr << buf.str() << std::endl;

    auto new_stack = getSupportingParallelReplicasQuery(updated_query_tree.get());

    //const auto & result_query_plan = planner.getQueryPlan();
    const auto & mapping = planner.getQueryNodeToPlanStepMapping();

    // for (const auto & [k, v] : mapping)
    //     std::cerr << "----- " << v->step->getName() << '\n' << reinterpret_cast<const void *>(k) << std::endl;

    const auto * res = findParallelReplicasQuery(new_stack, mapping);
    // if (res)
    //     std::cerr << "Result subtree " << res->dumpTree() << std::endl;
    // else
    //     std::cerr << "Result subtree is empty" << std::endl;

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
    while (query_tree_node)
    {
        auto join_tree_node_type = query_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
            {
                const auto & table_node = query_tree_node->as<TableNode &>();
                const auto & storage = table_node.getStorage();
                if (std::dynamic_pointer_cast<MergeTreeData>(storage) || typeid_cast<const StorageDummy *>(storage.get()))
                    return &table_node;

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
                if (const auto * res = findTableForParallelReplicas(join_node.getLeftTableExpression().get()))
                    return res;

                query_tree_node = join_node.getRightTableExpression().get();
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
    // const auto & settings = context->getSettingsRef();

    if (!context->canUseParallelReplicasOnFollower())
        return nullptr;

    return findTableForParallelReplicas(query_tree_node.get());
}

static void removeCTEs(ASTPtr & ast)
{
    std::stack<IAST *> stack;
    stack.push(ast.get());
    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (auto * subquery = typeid_cast<ASTSubquery *>(node))
            subquery->cte_name = {};

        for (const auto & child : node->children)
            stack.push(child.get());
    }
}

JoinTreeQueryPlan buildQueryPlanForParallelReplicas(
    const QueryNode & query_node,
    const PlannerContextPtr & planner_context,
    std::shared_ptr<const StorageLimitsList> storage_limits)
{
    // std::cerr << "buildQueryPlanForParallelReplicas 1 " << query_node.dumpTree() << std::endl;
    ASTPtr modified_query_ast;
    Block header;
    auto processed_stage = QueryProcessingStage::WithMergeableState;
    auto context = planner_context->getQueryContext();

    QueryTreeNodePtr modified_query_tree = query_node.clone();

    Block initial_header = InterpreterSelectQueryAnalyzer::getSampleBlock(
        modified_query_tree, context, SelectQueryOptions(processed_stage).analyze());

    rewriteJoinToGlobalJoin(modified_query_tree, context);
    // std::cerr << "buildQueryPlanForParallelReplicas 1 " << modified_query_tree->dumpTree() << std::endl;
    modified_query_tree = buildQueryTreeForShard(planner_context, modified_query_tree);
    // std::cerr << "buildQueryPlanForParallelReplicas 2 " << modified_query_tree->dumpTree() << std::endl;
    modified_query_ast = queryNodeToSelectQuery(modified_query_tree);
    removeCTEs(modified_query_ast);

    // std::cerr << "buildQueryPlanForParallelReplicas AST " << queryToString(modified_query_ast) << std::endl;
    // std::cerr << "buildQueryPlanForParallelReplicas AST " << modified_query_ast->dumpTree() << std::endl;

    // SelectQueryOptions opt(processed_stage);
    // Planner planner(modified_query_tree, opt, std::make_shared<GlobalPlannerContext>(nullptr));
    // planner.buildQueryPlanIfNeeded();
    // header = planner.getQueryPlan().getCurrentDataStream().header;

    // InterpreterSelectQueryAnalyzer interpreter(modified_query_tree, context, SelectQueryOptions(processed_stage));
    // header = interpreter.getSampleBlock();

    header = InterpreterSelectQueryAnalyzer::getSampleBlock(
        modified_query_tree, context, SelectQueryOptions(processed_stage).analyze());

    ClusterProxy::SelectStreamFactory select_stream_factory =
        ClusterProxy::SelectStreamFactory(
            header,
            {},
            {},
            processed_stage);

    QueryPlan query_plan;
    ClusterProxy::executeQueryWithParallelReplicas(
        query_plan,
        select_stream_factory,
        modified_query_ast,
        context,
        storage_limits);

    auto converting = ActionsDAG::makeConvertingActions(
        header.getColumnsWithTypeAndName(),
        initial_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position);

    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(converting));
    step->setStepDescription("Convert distributed names");
    query_plan.addStep(std::move(step));

    return {std::move(query_plan), std::move(processed_stage), {}, {}, {}};
}

}
