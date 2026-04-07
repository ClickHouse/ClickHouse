#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/**
 * Rewrites `sum(column +/- literal)` into two individual functions
 * `sum(column)` and `literal * count(column)`.
 * sum(column + literal) -> sum(column) + literal * count(column)
 * sum(literal + column) -> literal * count(column) + sum(column)
 * sum(column - literal) -> sum(column) - literal * count(column)
 * sum(literal - column) -> literal * count(column) - sum(column)
 */
class RewriteSumFunctionWithSumAndCountPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteSumFunctionWithSumAndCountPass"; }

    String getDescription() override { return "Rewrite sum(column +/- literal) into sum(column) and literal * count(column)"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
