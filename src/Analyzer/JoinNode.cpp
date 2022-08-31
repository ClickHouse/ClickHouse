#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

JoinNode::JoinNode(QueryTreeNodePtr left_table_expression_,
    QueryTreeNodePtr right_table_expression_,
    QueryTreeNodePtr join_expression_,
    JoinLocality locality_,
    JoinStrictness strictness_,
    JoinKind kind_)
    : locality(locality_)
    , strictness(strictness_)
    , kind(kind_)
{
    children.resize(children_size);
    children[left_table_expression_child_index] = std::move(left_table_expression_);
    children[right_table_expression_child_index] = std::move(right_table_expression_);
    children[join_expression_child_index] = std::move(join_expression_);
}

JoinNode::JoinNode(QueryTreeNodePtr left_table_expression_,
    QueryTreeNodePtr right_table_expression_,
    QueryTreeNodes using_identifiers,
    JoinLocality locality_,
    JoinStrictness strictness_,
    JoinKind kind_)
    : locality(locality_)
    , strictness(strictness_)
    , kind(kind_)
{
    children.resize(children_size);
    children[left_table_expression_child_index] = std::move(left_table_expression_);
    children[right_table_expression_child_index] = std::move(right_table_expression_);
    children[join_expression_child_index] = std::make_shared<ListNode>(std::move(using_identifiers));
}

JoinNode::JoinNode(JoinLocality locality_,
    JoinStrictness strictness_,
    JoinKind kind_)
    : locality(locality_)
    , strictness(strictness_)
    , kind(kind_)
{}

void JoinNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "JOIN id: " << format_state.getNodeId(this);

    if (locality != JoinLocality::Unspecified)
        buffer << ", locality: " << toString(locality);

    if (strictness != JoinStrictness::Unspecified)
        buffer << ", strictness: " << toString(strictness);

    buffer << ", kind: " << toString(kind);

    buffer << '\n' << std::string(indent + 2, ' ') << "LEFT TABLE EXPRESSION\n";
    getLeftTableExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    buffer << '\n' << std::string(indent + 2, ' ') << "RIGHT TABLE EXPRESSSION\n";
    getRightTableExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    if (getJoinExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "JOIN EXPRESSSION\n";
        getJoinExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool JoinNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const JoinNode &>(rhs);
    return locality == rhs_typed.locality && strictness == rhs_typed.strictness && kind == rhs_typed.kind;
}

void JoinNode::updateTreeHashImpl(HashState & state) const
{
    state.update(locality);
    state.update(strictness);
    state.update(kind);
}

ASTPtr JoinNode::toASTTableJoin() const
{
    auto join_ast = std::make_shared<ASTTableJoin>();
    join_ast->locality = locality;
    join_ast->strictness = strictness;
    join_ast->kind = kind;

    if (children[join_expression_child_index])
    {
        auto join_expression_ast = children[join_expression_child_index]->toAST();

        if (children[join_expression_child_index]->getNodeType() == QueryTreeNodeType::LIST)
            join_ast->using_expression_list = std::move(join_expression_ast);
        else
            join_ast->on_expression = std::move(join_expression_ast);
    }

    return join_ast;
}

ASTPtr JoinNode::toASTImpl() const
{
    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();

    addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, children[left_table_expression_child_index]);

    size_t join_table_index = tables_in_select_query_ast->children.size();

    auto join_ast = std::make_shared<ASTTableJoin>();
    join_ast->locality = locality;
    join_ast->strictness = strictness;
    join_ast->kind = kind;

    if (children[join_expression_child_index])
    {
        auto join_expression_ast = children[join_expression_child_index]->toAST();

        if (children[join_expression_child_index]->getNodeType() == QueryTreeNodeType::LIST)
            join_ast->using_expression_list = std::move(join_expression_ast);
        else
            join_ast->on_expression = std::move(join_expression_ast);
    }

    addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, children[right_table_expression_child_index]);

    auto & table_element = tables_in_select_query_ast->children.at(join_table_index)->as<ASTTablesInSelectQueryElement &>();
    table_element.children.push_back(std::move(join_ast));
    table_element.table_join = table_element.children.back();

    return tables_in_select_query_ast;
}

QueryTreeNodePtr JoinNode::cloneImpl() const
{
    JoinNodePtr result_join_node(new JoinNode(locality, strictness, kind));
    return result_join_node;
}

}
