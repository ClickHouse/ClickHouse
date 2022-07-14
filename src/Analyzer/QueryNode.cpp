#include <Analyzer/QueryNode.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

QueryNode::QueryNode()
{
    children.resize(children_size);
    children[with_child_index] = std::make_shared<ListNode>();
    children[projection_child_index] = std::make_shared<ListNode>();
}

void QueryNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "QUERY ";
    writePointerHex(this, buffer);
    buffer << '\n';

    if (!getWith().getNodes().empty())
    {
        buffer << std::string(indent, ' ') << "WITH\n";
        getWith().dumpTree(buffer, indent + 2);
        buffer << '\n';
    }

    buffer << std::string(indent, ' ') << "PROJECTION\n";
    getProjection().dumpTree(buffer, indent + 2);
    buffer << '\n';

    if (getFrom())
    {
        buffer << std::string(indent, ' ') << "JOIN TREE\n";
        getFrom()->dumpTree(buffer, indent + 2);
        buffer << '\n';
    }

    if (getPrewhere())
    {
        buffer << std::string(indent, ' ') << "PREWHERE\n";
        getPrewhere()->dumpTree(buffer, indent + 2);
        buffer << '\n';
    }

    if (getWhere())
    {
        buffer << std::string(indent, ' ') << "WHERE\n";
        getWhere()->dumpTree(buffer, indent + 2);
        buffer << '\n';
    }
}

void QueryNode::updateTreeHashImpl(HashState &) const
{
    /// TODO: No state
}

ASTPtr QueryNode::toASTImpl() const
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    if (!getWith().getNodes().empty())
        select_query->setExpression(ASTSelectQuery::Expression::WITH, getWithNode()->toAST());

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, children[projection_child_index]->toAST());

    auto table_expression_ast = std::make_shared<ASTTableExpression>();
    table_expression_ast->children.push_back(children[from_child_index]->toAST());
    table_expression_ast->database_and_table_name = table_expression_ast->children.back();

    auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
    tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

    auto tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();
    tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query_ast));

    if (getPrewhere())
        select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, getPrewhere()->toAST());

    if (getWhere())
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, getWhere()->toAST());

    return select_query;
}

QueryTreeNodePtr QueryNode::cloneImpl() const
{
    return std::make_shared<QueryNode>();
}

}
