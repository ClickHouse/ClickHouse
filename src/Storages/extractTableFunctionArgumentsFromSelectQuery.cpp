#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>


namespace DB
{

ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query)
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query || !select_query->tables())
        return nullptr;

    auto * tables = select_query->tables()->as<ASTTablesInSelectQuery>();
    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();
    if (!table_expression->table_function)
        return nullptr;

    return table_expression->table_function->as<ASTFunction>();
}

}
