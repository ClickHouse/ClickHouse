#include <memory>
#include <Analyzer/Passes/InjectRandomOrderIfNoOrderByPass.h>

#include <Analyzer/IQueryTreePass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/UnionNode.h>

#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include "Analyzer/ColumnNode.h"

namespace DB
{

namespace Setting
{
    extern const SettingsBool inject_random_order_for_select_without_order_by;
}

/// Utility: append ORDER BY rand() to an order-by list
void addRandomOrderBy(ListNode & order_by_list_node, ContextPtr context)
{
    /// Build rand() node
    auto & function_factory = FunctionFactory::instance();
    auto resolver = function_factory.get("rand", context);
    auto rand_function_node = std::make_shared<FunctionNode>("rand");
    rand_function_node->resolveAsFunction(resolver);

    /// Create and add ORDER BY rand() node
    auto sort_node = std::make_shared<SortNode>(rand_function_node);
    order_by_list_node.getNodes().push_back(std::move(sort_node));
}

void wrapWithSelectOrderBy(QueryTreeNodePtr & query_root, ContextPtr context) {
    auto * query_node = query_root->as<QueryNode>();

    /// Re-resolve query_node columns setting the unique alias
    String unique_column_name = "__subquery_column_" + toString(UUIDHelpers::generateV4());
    auto subquery_projection_columns = query_node->getProjectionColumns();
    query_node->clearProjectionColumns();
    query_node->setProjectionAliasesToOverride({unique_column_name});
    query_node->resolveProjectionColumns(subquery_projection_columns);

    /// SELECT * AS _unique_name_ FROM query_node order by rand()
    auto new_root = std::make_shared<QueryNode>(Context::createCopy(context));
    new_root->setIsSubquery(true);
    new_root->getJoinTree() = query_root;
    NameAndTypePair column{unique_column_name, subquery_projection_columns[0].type};
    new_root->getProjection().getNodes().push_back(std::make_shared<ColumnNode>(column, query_root));
    new_root->resolveProjectionColumns({column});
    addRandomOrderBy(new_root->getOrderBy(), context);

    // replace old root with new wrapping query node
    query_root = new_root;
}

/// If inject_random_order_for_select_without_order_by = 1, then inject `ORDER BY rand()` into top level query.
/// The goal is to expose flaky tests during testing.
void InjectRandomOrderIfNoOrderByPass::run(QueryTreeNodePtr & root, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::inject_random_order_for_select_without_order_by])
        return;

    /// Case 1: Top-level SELECT
    if (auto * query_node = root->as<QueryNode>())
    {
        if (!query_node->hasOrderBy()) 
            wrapWithSelectOrderBy(root, context);
            
        return;
    }

    /// Case 2: Top-level UNION - inject `ORDER BY rand()` into each branch
    if (auto * union_node = root->as<UnionNode>())
    {
        auto & union_branches = union_node->getQueries();
        for (auto & unio_branch_node : union_branches.getNodes())
        {
            if (auto * branch_node = unio_branch_node->as<QueryNode>())
                if (!branch_node->hasOrderBy())
                    wrapWithSelectOrderBy(unio_branch_node, context);
        }
        return;
    }

}

}
