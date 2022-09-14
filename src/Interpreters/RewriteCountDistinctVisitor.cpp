#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/RewriteCountDistinctVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include "Coordination/KeeperStorage.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTSubquery.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include <Parsers/Lexer.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

void RewriteCountDistinctFunctionMatcher::visit(ASTPtr & ast, Data & /*data*/)
{
    auto * selectq = ast->as<ASTSelectQuery>();
    if (!selectq || !selectq->tables() || selectq->tables()->children.size() != 1)
        return;
    auto expr_list = selectq->select();
    if (!expr_list || expr_list->children.size() != 1)
        return;
    auto * func = expr_list->children[0]->as<ASTFunction>();
    if (!func || (Poco::toLower(func->name) != "countdistinct" && Poco::toLower(func->name) != "uniqexact"))
        return;
    auto arg = func->arguments->children;
    if (arg.size() != 1)
        return;
    if (!arg[0]->as<ASTIdentifier>())
        return;
    if (selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children.size() != 1)
        return;
    auto * table_expr = selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children[0]->as<ASTTableExpression>();
    if (!table_expr || table_expr->size() != 1 || !table_expr->database_and_table_name)
        return;
    // Check done, we now rewrite the AST
    auto cloned_select_query = selectq->clone();
    expr_list->children[0] = makeASTFunction("count");

    auto table_name = table_expr->database_and_table_name->as<ASTTableIdentifier>()->name();
    table_expr->children.clear();
    table_expr->children.emplace_back(std::make_shared<ASTSubquery>());
    table_expr->database_and_table_name = nullptr;
    table_expr->table_function = nullptr;
    table_expr->subquery = table_expr->children[0];

    auto column_name = arg[0]->as<ASTIdentifier>()->name();
    // Form AST for subquery
    {
        auto * select_ptr = cloned_select_query->as<ASTSelectQuery>();
        select_ptr->refSelect()->children.clear();
        select_ptr->refSelect()->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
        auto exprlist = std::make_shared<ASTExpressionList>();
        exprlist->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
        cloned_select_query->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, exprlist);

        auto expr = std::make_shared<ASTExpressionList>();
        expr->children.emplace_back(cloned_select_query);
        auto select_with_union = std::make_shared<ASTSelectWithUnionQuery>();
        select_with_union->union_mode = SelectUnionMode::Unspecified;
        select_with_union->is_normalized = false;
        select_with_union->list_of_modes.clear();
        select_with_union->set_of_modes.clear();
        select_with_union->children.emplace_back(expr);
        select_with_union->list_of_selects = expr;
        table_expr->children[0]->as<ASTSubquery>()->children.emplace_back(select_with_union);
    }
}

}
