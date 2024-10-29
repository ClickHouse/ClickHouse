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
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/buildQueryTreeForShard.h>
#include "Processors/QueryPlan/SortingStep.h"

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
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                const auto & table_node = query_tree_node->as<TableNode &>();
                const auto & storage = table_node.getStorage();
                /// Here we check StorageDummy as well, to support a query tree with replaced storages.
                if (std::dynamic_pointer_cast<MergeTreeData>(storage) || typeid_cast<const StorageDummy *>(storage.get()))
                {
                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    /// parallel replicas is not supported with FINAL
                    if (table_node.getTableExpressionModifiers() && table_node.getTableExpressionModifiers()->hasFinal())
                    {
                        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                        return {};
                    }

                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    return res;
                }

                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                return {};
            }
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                return {};
            }
            case QueryTreeNodeType::QUERY:
            {
                const auto & query_node_to_process = query_tree_node->as<QueryNode &>();
                query_tree_node = query_node_to_process.getJoinTree().get();
                res.push(&query_node_to_process);
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                const auto & union_node = query_tree_node->as<UnionNode &>();
                const auto & union_queries = union_node.getQueries().getNodes();

                if (union_queries.empty())
                {
                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    return {};
                }

                query_tree_node = union_queries.front().get();
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                const auto & array_join_node = query_tree_node->as<ArrayJoinNode &>();
                query_tree_node = array_join_node.getTableExpression().get();
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
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
                {
                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    return {};
                }

                query_tree_node = join_node.getLeftTableExpression().get();
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
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

    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
    while (!stack.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        const QueryNode * subquery_node = stack.top();
        stack.pop();

        auto it = mapping.find(subquery_node);
        /// This should not happen ideally.
        if (it == mapping.end())
        {
            LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            break;
        }

        const QueryPlan::Node * const curr_node = it->second;
        std::deque<std::pair<const QueryPlan::Node *, bool>> nodes_to_check;
        nodes_to_check.push_front(std::make_pair(curr_node, false));
        bool can_distribute_full_node = true;
        bool in = false;

        while (!nodes_to_check.empty() /* && nodes_to_check.front() != prev_checked_node*/)
        {
            LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            const auto & [next_node_to_check, digging_into_rabbit_hole] = nodes_to_check.front();
            LOG_DEBUG(
                &Poco::Logger::get("debug"),
                "next_node_to_check->step->getName()={}, next_node_to_check->step->getStepDescription());={}",
                next_node_to_check->step->getName(),
                next_node_to_check->step->getStepDescription());
            nodes_to_check.pop_front();
            const auto & children = next_node_to_check->children;
            auto * step = next_node_to_check->step.get();

            if (children.empty())
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                /// Found a source step. This should be possible only in the first iteration.
                if (prev_checked_node)
                {
                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    // return nullptr;
                }

                nodes_to_check = {};
            }
            else if (children.size() == 1)
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                const auto * expression = typeid_cast<ExpressionStep *>(step);
                const auto * filter = typeid_cast<FilterStep *>(step);
                const auto * sorting = typeid_cast<SortingStep *>(step);

                const auto * creating_sets = typeid_cast<DelayedCreatingSetsStep *>(step);
                bool allowed_creating_sets = settings[Setting::parallel_replicas_allow_in_with_subquery] && creating_sets;

                if (!expression && !filter && !allowed_creating_sets && !(sorting && sorting->getStepDescription().contains("before JOIN")))
                {
                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    can_distribute_full_node = false;
                    in = digging_into_rabbit_hole;
                }

                nodes_to_check.push_front(std::pair(children.front(), digging_into_rabbit_hole));
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                const auto * join = typeid_cast<JoinStep *>(step);
                /// We've checked that JOIN is INNER/LEFT in query tree.
                /// Don't distribute UNION node.
                if (!join)
                {
                    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                    return res;
                }

                for (const auto & child : children)
                    nodes_to_check.push_front(std::make_pair(child, true));
            }
        }

        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);

        /// Current node contains steps like GROUP BY / DISTINCT
        /// Will try to execute query up to WithMergableStage
        if (!can_distribute_full_node)
        {
            /// Current query node does not contain subqueries.
            /// We can execute parallel replicas over storage::read.
            LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            if (!res)
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                return nullptr;
            }

            return in ? res : subquery_node;
        }

        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);

        /// Query is simple enough to be fully distributed.
        res = subquery_node;
        prev_checked_node = curr_node;
    }

    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
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
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        return nullptr;
    }

    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
    auto stack = getSupportingParallelReplicasQuery(query_tree_node.get());
    /// Empty stack means that storage does not support parallel replicas.
    if (stack.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        return nullptr;
    }

    /// We don't have any subquery and storage can process parallel replicas by itself.
    if (stack.top() == query_tree_node.get())
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        return nullptr;
    }

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
        LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        while (!new_stack.empty())
        {
            LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            if (res == new_stack.top())
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
                return stack.top();
            }

            stack.pop();
            new_stack.pop();
        }
    }

    LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
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
    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(converting));
    step->setStepDescription("Convert distributed names");
    query_plan.addStep(std::move(step));

    return {std::move(query_plan), std::move(processed_stage), {}, {}, {}};
}

}
