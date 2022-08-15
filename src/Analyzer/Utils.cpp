#include <Analyzer/Utils.h>

#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>

#include <Analyzer/IdentifierNode.h>

namespace DB
{

bool isNodePartOfTree(const IQueryTreeNode * node, const IQueryTreeNode * root)
{
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        const auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (subtree_node == node)
            return true;

        for (const auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return false;
}

bool isNameOfInFunction(const std::string & function_name)
{
    bool is_special_function_in = function_name == "in" || function_name == "globalIn" || function_name == "notIn" || function_name == "globalNotIn" ||
        function_name == "nullIn" || function_name == "globalNullIn" || function_name == "notNullIn" || function_name == "globalNotNullIn" ||
        function_name == "inIgnoreSet" || function_name == "globalInIgnoreSet" || function_name == "notInIgnoreSet" || function_name == "globalNotInIgnoreSet" ||
        function_name == "nullInIgnoreSet" || function_name == "globalNullInIgnoreSet" || function_name == "notNullInIgnoreSet" || function_name == "globalNotNullInIgnoreSet";

    return is_special_function_in;
}

bool isTableExpression(const IQueryTreeNode * node)
{
    auto node_type = node->getNodeType();
    return node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::TABLE_FUNCTION || node_type == QueryTreeNodeType::QUERY;
}

static ASTPtr convertIntoTableExpressionAST(const QueryTreeNodePtr & table_expression_node)
{
    ASTPtr table_expression_node_ast;
    auto node_type = table_expression_node->getNodeType();

    if (node_type == QueryTreeNodeType::IDENTIFIER)
    {
        const auto & identifier_node = table_expression_node->as<IdentifierNode &>();
        const auto & identifier = identifier_node.getIdentifier();

        if (identifier.getPartsSize() == 1)
            table_expression_node_ast = std::make_shared<ASTTableIdentifier>(identifier[0]);
        else if (identifier.getPartsSize() == 2)
            table_expression_node_ast = std::make_shared<ASTTableIdentifier>(identifier[0], identifier[1]);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Identifier for table expression must contain 1 or 2 parts. Actual {}",
                identifier.getFullName());
    }
    else
    {
        table_expression_node_ast = table_expression_node->toAST();
    }

    auto result_table_expression = std::make_shared<ASTTableExpression>();
    result_table_expression->children.push_back(table_expression_node_ast);

    if (node_type == QueryTreeNodeType::QUERY)
        result_table_expression->subquery = result_table_expression->children.back();
    else if (node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::IDENTIFIER)
        result_table_expression->database_and_table_name = result_table_expression->children.back();
    else if (node_type == QueryTreeNodeType::TABLE_FUNCTION)
        result_table_expression->table_function = result_table_expression->children.back();
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected identiifer, table, query, or table function. Actual {}", table_expression_node->formatASTForErrorMessage());

    return result_table_expression;
}

void addTableExpressionIntoTablesInSelectQuery(ASTPtr & tables_in_select_query_ast, const QueryTreeNodePtr & table_expression)
{
    auto table_expression_node_type = table_expression->getNodeType();

    switch (table_expression_node_type)
    {
        case QueryTreeNodeType::IDENTIFIER:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            auto table_expression_ast = convertIntoTableExpressionAST(table_expression);

            auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
            tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
            tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

            tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));
            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
            [[fallthrough]];
        case QueryTreeNodeType::JOIN:
        {
            auto table_expression_tables_in_select_query_ast = table_expression->toAST();
            tables_in_select_query_ast->children.reserve(table_expression_tables_in_select_query_ast->children.size());
            for (auto && left_table_element_ast : table_expression_tables_in_select_query_ast->children)
                tables_in_select_query_ast->children.push_back(std::move(left_table_element_ast));
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected node type for table expression. Expected table, query, table function, join or array join. Actual {}",
                table_expression->getNodeTypeName());
        }
    }
}

}
