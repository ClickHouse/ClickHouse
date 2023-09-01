#include <Storages/addColumnsStructureToQueryWithClusterEngine.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query)
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query || !select_query->tables())
        return nullptr;

    auto * tables = select_query->tables()->as<ASTTablesInSelectQuery>();
    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();
    if (!table_expression->table_function)
        return nullptr;

    auto * table_function = table_expression->table_function->as<ASTFunction>();
    return table_function->arguments->as<ASTExpressionList>();
}

void addColumnsStructureToQueryWithClusterEngine(ASTPtr & query, const String & structure, size_t max_arguments, const String & function_name)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function {}, got '{}'", function_name, queryToString(query));
    auto structure_literal = std::make_shared<ASTLiteral>(structure);

    if (expression_list->children.size() < 2 || expression_list->children.size() > max_arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 2 to {} arguments in {} table functions, got {}",
                        function_name, max_arguments, expression_list->children.size());

    if (expression_list->children.size() == 2 || expression_list->children.size() == max_arguments - 1)
    {
        auto format_literal = std::make_shared<ASTLiteral>("auto");
        expression_list->children.push_back(format_literal);
    }

    expression_list->children.push_back(structure_literal);
}

}
