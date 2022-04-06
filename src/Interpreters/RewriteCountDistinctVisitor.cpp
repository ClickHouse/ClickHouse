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

namespace DB
{

void RewriteCountDistinctFunctionMatcher::visit(ASTPtr & ast, Data & /*data*/)
{
    auto * selectq = ast->as<ASTSelectQuery>();
    if (!selectq)
        return;
    auto expr_list = selectq->select();
    if (!expr_list)
    {
        return;
    }
    if (expr_list->children.size() != 1)
        return;
    auto * func = expr_list->children[0]->as<ASTFunction>();
    if (!func || (Poco::toLower(func->name) != "countdistinct" && Poco::toLower(func->name) != "uniqexact"))
        return;
    auto arg = func->arguments->children;
    if (arg.size() != 1)
        return;
    auto column_name = arg[0]->as<ASTIdentifier>()->name();
    func->name = "count";
    func->children.clear();
    func->children.emplace_back(std::make_shared<ASTExpressionList>());
    func->arguments = func->children[0];
    func->parameters = nullptr;
    if (!selectq->tables() || selectq->tables()->children.size() != 1)
        return;
    auto table_name = selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children[0]->as<ASTTableExpression>()->children[0]->as<ASTTableIdentifier>()->name();
    auto * te = selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children[0]->as<ASTTableExpression>();
    te->children.clear();
    te->children.emplace_back(std::make_shared<ASTSubquery>());
    te->database_and_table_name = nullptr;
    te->table_function = nullptr;
    te->subquery = te->children[0];

    std::string sub_sql = "SELECT " + column_name + " FROM " + table_name +  " GROUP BY " + column_name + "  ";
    ParserQuery parser(&sub_sql[sub_sql.length()-1]);
    auto parsed_ast = parseQuery(parser, &sub_sql[0], &sub_sql[sub_sql.length()-1], "", 1024, 1024);
    te->children[0]->as<ASTSubquery>()->children.clear();
    te->children[0]->as<ASTSubquery>()->children.emplace_back(parsed_ast);
}

}
