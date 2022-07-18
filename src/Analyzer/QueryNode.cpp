#include <Analyzer/QueryNode.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Common/SipHash.h>

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

    if (hasAlias())
        buffer << " alias : " << getAlias();

    buffer << " subquery : " << is_subquery;
    buffer << " cte : " << is_cte;
    buffer << " cte_name : " << cte_name;

    if (!getWith().getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "WITH\n";
        getWith().dumpTree(buffer, indent + 4);
    }

    buffer << '\n';
    buffer << std::string(indent + 2, ' ') << "PROJECTION\n";
    getProjection().dumpTree(buffer, indent + 4);

    if (getFrom())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "JOIN TREE\n";
        getFrom()->dumpTree(buffer, indent + 4);
    }

    if (getPrewhere())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "PREWHERE\n";
        getPrewhere()->dumpTree(buffer, indent + 4);
    }

    if (getWhere())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "WHERE\n";
        getWhere()->dumpTree(buffer, indent + 4);
    }
}

bool QueryNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const QueryNode &>(rhs);
    return is_subquery == rhs_typed.is_subquery && cte_name == rhs_typed.cte_name;
}

void QueryNode::updateTreeHashImpl(HashState & state) const
{
    state.update(is_subquery);
    state.update(is_cte);

    state.update(cte_name.size());
    state.update(cte_name);
}

ASTPtr QueryNode::toASTImpl() const
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    if (!getWith().getNodes().empty())
        select_query->setExpression(ASTSelectQuery::Expression::WITH, getWithNode()->toAST());

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, children[projection_child_index]->toAST());

    auto table_expression_ast = std::make_shared<ASTTableExpression>();
    table_expression_ast->children.push_back(children[from_child_index]->toAST());

    if (children[from_child_index]->getNodeType() == QueryTreeNodeType::TABLE)
        table_expression_ast->database_and_table_name = table_expression_ast->children.back();
    else if (children[from_child_index]->getNodeType() == QueryTreeNodeType::QUERY)
        table_expression_ast->subquery = table_expression_ast->children.back();

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

    auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();
    result_select_query->union_mode = SelectUnionMode::Unspecified;

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));

    result_select_query->list_of_selects = std::move(list_of_selects);
    result_select_query->children.push_back(std::move(select_query));

    if (is_subquery)
    {
        auto subquery = std::make_shared<ASTSubquery>();

        subquery->cte_name = cte_name;
        subquery->children.push_back(std::move(result_select_query));

        return subquery;
    }

    return result_select_query;
}

QueryTreeNodePtr QueryNode::cloneImpl() const
{
    auto result_query_node = std::make_shared<QueryNode>();

    result_query_node->is_subquery = is_subquery;
    result_query_node->is_cte = is_cte;
    result_query_node->cte_name = cte_name;

    return result_query_node;
}

}
