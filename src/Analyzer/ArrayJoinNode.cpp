#include <Analyzer/ArrayJoinNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

ArrayJoinNode::ArrayJoinNode(QueryTreeNodePtr table_expression_, QueryTreeNodePtr join_expressions_, bool is_left_)
    : is_left(is_left_)
{
    children.resize(children_size);
    children[table_expression_child_index] = std::move(table_expression_);
    children[join_expressions_child_index] = std::move(join_expressions_);
}

void ArrayJoinNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "ARRAY_JOIN id: " << format_state.getNodeId(this);
    buffer << ", is_left: " << is_left;

    buffer << '\n' << std::string(indent + 2, ' ') << "TABLE EXPRESSION\n";
    getTableExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    buffer << '\n' << std::string(indent + 2, ' ') << "JOIN EXPRESSSIONS\n";
    getJoinExpressionsNode()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool ArrayJoinNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const ArrayJoinNode &>(rhs);
    return is_left == rhs_typed.is_left;
}

void ArrayJoinNode::updateTreeHashImpl(HashState & state) const
{
    state.update(is_left);
}

ASTPtr ArrayJoinNode::toASTImpl() const
{
    auto array_join_ast = std::make_shared<ASTArrayJoin>();
    array_join_ast->kind = is_left ? ASTArrayJoin::Kind::Left : ASTArrayJoin::Kind::Inner;

    const auto & join_expression_list_node = getJoinExpressionsNode();
    array_join_ast->children.push_back(join_expression_list_node->toAST());
    array_join_ast->expression_list = array_join_ast->children.back();

    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();
    addTableExpressionIntoTablesInSelectQuery(tables_in_select_query_ast, children[table_expression_child_index]);

    auto array_join_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
    array_join_query_element_ast->children.push_back(std::move(array_join_ast));
    array_join_query_element_ast->array_join = array_join_query_element_ast->children.back();

    tables_in_select_query_ast->children.push_back(std::move(array_join_query_element_ast));

    return tables_in_select_query_ast;
}

QueryTreeNodePtr ArrayJoinNode::cloneImpl() const
{
    ArrayJoinNodePtr result_array_join_node(new ArrayJoinNode(is_left));
    return result_array_join_node;
}

}
