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

namespace DB
{

namespace Setting
{
    extern const SettingsBool inject_random_order_for_select_without_order_by;
}

/// Utility: append ORDER BY rand() to an order-by list
void addRandomOrderBy(ListNode & order_by_list, ContextPtr context)
{
    /// Build rand() with empty arg list
    auto rand_fn = std::make_shared<FunctionNode>("rand");
    auto & ff = FunctionFactory::instance();
    auto resolver = ff.get("rand", context);
    rand_fn->resolveAsFunction(std::move(resolver));

    /// Create and add one SortNode
    auto sort = std::make_shared<SortNode>(rand_fn);
    order_by_list.getNodes().push_back(std::move(sort));
}

/// If inject_random_order_for_select_without_order_by = 1, then inject `ORDER BY rand()` into top level query.
/// The goal is to expose flaky tests during testing.
void InjectRandomOrderIfNoOrderByPass::run(QueryTreeNodePtr & root, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::inject_random_order_for_select_without_order_by])
        return;

    /// Case 1: top-level SELECT
    if (auto * q = root->as<QueryNode>())
    {
        if (!q->hasOrderBy())
            addRandomOrderBy(q->getOrderBy(), context);
        return;
    }

    /// Case 2: top-level UNION - inject into each branch
    if (auto * u = root->as<UnionNode>())
    {
        auto & branches = u->getQueries();
        for (auto & branch_ptr : branches.getNodes())
        {
            if (auto * branch_q = branch_ptr->as<QueryNode>())
                if (!branch_q->hasOrderBy())
                    addRandomOrderBy(branch_q->getOrderBy(), context);
        }
        return;
    }

}

}
