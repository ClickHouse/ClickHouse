#include <Storages/extractTableFunctionFromSelectQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB
{

ASTTableExpression * extractTableExpressionASTPtrFromSelectQuery(ASTPtr & query)
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query || !select_query->tables())
        return nullptr;

    auto * tables = select_query->tables()->as<ASTTablesInSelectQuery>();
    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();
    return table_expression;
}

ASTPtr extractTableFunctionASTPtrFromSelectQuery(ASTPtr & query)
{
    auto table_expression = extractTableExpressionASTPtrFromSelectQuery(query);
    return table_expression ? table_expression->table_function : nullptr;
}

ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query)
{
    auto table_function_ast = extractTableFunctionASTPtrFromSelectQuery(query);
    if (!table_function_ast)
        return nullptr;

    auto * table_function = table_function_ast->as<ASTFunction>();
    return table_function;
}

ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query)
{
    auto * table_function = extractTableFunctionFromSelectQuery(query);
    if (!table_function)
        return nullptr;
    return table_function->arguments->as<ASTExpressionList>();
}

}
