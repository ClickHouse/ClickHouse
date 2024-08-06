#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/assert_cast.h>

namespace DB
{

ArrayJoinNode::ArrayJoinNode(QueryTreeNodePtr table_expression_, QueryTreeNodePtr join_expressions_, bool is_left_)
    : IQueryTreeNode(children_size)
    , is_left(is_left_)
{
    children[table_expression_child_index] = std::move(table_expression_);
    children[join_expressions_child_index] = std::move(join_expressions_);
}

void ArrayJoinNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "ARRAY_JOIN id: " << format_state.getNodeId(this);
    buffer << ", is_left: " << is_left;

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << '\n' << std::string(indent + 2, ' ') << "TABLE EXPRESSION\n";
    getTableExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    buffer << '\n' << std::string(indent + 2, ' ') << "JOIN EXPRESSIONS\n";
    getJoinExpressionsNode()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool ArrayJoinNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const ArrayJoinNode &>(rhs);
    return is_left == rhs_typed.is_left;
}

void ArrayJoinNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    state.update(is_left);
}

QueryTreeNodePtr ArrayJoinNode::cloneImpl() const
{
    return std::make_shared<ArrayJoinNode>(getTableExpression(), getJoinExpressionsNode(), is_left);
}

ASTPtr ArrayJoinNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto array_join_ast = std::make_shared<ASTArrayJoin>();
    array_join_ast->kind = is_left ? ASTArrayJoin::Kind::Left : ASTArrayJoin::Kind::Inner;

    auto array_join_expressions_ast = std::make_shared<ASTExpressionList>();
    const auto & array_join_expressions = getJoinExpressions().getNodes();

    for (const auto & array_join_expression : array_join_expressions)
    {
        ASTPtr array_join_expression_ast;

        auto * column_node = array_join_expression->as<ColumnNode>();
        if (column_node && column_node->getExpression())
        {
            if (const auto * function_node = column_node->getExpression()->as<FunctionNode>(); function_node && function_node->getFunctionName() == "nested")
                array_join_expression_ast = array_join_expression->toAST(options);
            else
                array_join_expression_ast = column_node->getExpression()->toAST(options);
        }
        else
            array_join_expression_ast = array_join_expression->toAST(options);

        array_join_expression_ast->setAlias(array_join_expression->getAlias());
        array_join_expressions_ast->children.push_back(std::move(array_join_expression_ast));
    }

    array_join_ast->children.push_back(std::move(array_join_expressions_ast));
    array_join_ast->expression_list = array_join_ast->children.back();

    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();
    addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, children[table_expression_child_index], options);

    auto array_join_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
    array_join_query_element_ast->children.push_back(std::move(array_join_ast));
    array_join_query_element_ast->array_join = array_join_query_element_ast->children.back();

    tables_in_select_query_ast->children.push_back(std::move(array_join_query_element_ast));

    return tables_in_select_query_ast;
}

}
