#include <Interpreters/RewriteOrderByVisitor.hpp>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

void RewriteOrderBy::visit(ASTPtr & ast, Data &)
{
    auto * query = ast->as<ASTSelectQuery>();
    if (!query)
        return;

    const ASTPtr & order_by = query->orderBy();
    if (!order_by)
        return;

    const auto * expr_list = order_by->as<ASTExpressionList>();
    if (!expr_list)
        return;

    if (expr_list->children.size() != 1)
        return;

    const auto * order_by_elem = expr_list->children.front()->as<ASTOrderByElement>();
    if (!order_by_elem)
        return;

    const auto * func = order_by_elem->children.front()->as<ASTFunction>();
    if (!func || func->name != "tuple")
        return;

    if (const auto * inner_list = func->children.front()->as<ASTExpressionList>())
    {
        auto new_order_by = std::make_shared<ASTExpressionList>();
        for (const auto & identifier : inner_list->children)
        {
            // clone w/o children
            auto clone = std::make_shared<ASTOrderByElement>(*order_by_elem);

            clone->children[0] = identifier;
            new_order_by->children.emplace_back(clone);
        }
        if (!new_order_by->children.empty())
            query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(new_order_by));
    }
}

}

