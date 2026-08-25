#include <Analyzer/JoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Utils.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/assert_cast.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinNode::JoinNode(QueryTreeNodePtr left_table_expression_,
    QueryTreeNodePtr right_table_expression_,
    QueryTreeNodePtr join_expression_,
    JoinLocality locality_,
    JoinStrictness strictness_,
    JoinKind kind_,
    bool is_using_join_expression_)
    : IQueryTreeNode(children_size)
    , locality(locality_)
    , strictness(strictness_)
    , kind(kind_)
    , is_using_join_expression(is_using_join_expression_)
{
    children[left_table_expression_child_index] = std::move(left_table_expression_);
    children[right_table_expression_child_index] = std::move(right_table_expression_);
    children[join_expression_child_index] = std::move(join_expression_);
}

/// There is a special workaround for the case when ARRAY JOIN alias is used in USING statement.
/// Example: ... ARRAY JOIN arr AS dummy INNER JOIN system.one USING (dummy);
///
/// In case of ARRAY JOIN, the column is renamed, so the query tree will look like:
/// JOIN EXPRESSION
/// LIST
///   COLUMN id: 16, column_name: dummy
///     EXPRESSION
///       LIST
///         COLUMN id: 18, column_name: __array_join_exp_1
///         COLUMN id: 19, column_name: dummy
///
/// Previously, when we convert QueryTree back to ast, the query would look like:
/// ARRAY JOIN arr AS __array_join_exp_1 ALL INNER JOIN system.one USING (__array_join_exp_1)
/// Which is incorrect query (which is broken in distributed case) because system.one do not have __array_join_exp_1.
///
/// In order to mitigate this, the syntax 'USING (__array_join_exp_1 AS dummy)' is introduced,
/// which means that '__array_join_exp_1' is taken from left, 'dummy' is taken from right,
/// and the USING column name is also 'dummy'
///
/// See 03448_analyzer_array_join_alias_in_join_using_bug
static ASTPtr tryMakeUsingColumnASTWithAlias(const QueryTreeNodePtr & node)
{
    const auto * column_node = node->as<ColumnNode>();
    if (!column_node)
        return nullptr;

    const auto & expr = column_node->getExpression();
    if (!expr)
        return nullptr;

    const auto * expr_list_node = expr->as<ListNode>();
    if (!expr_list_node)
        return nullptr;

    if (expr_list_node->getNodes().size() != 2)
        return nullptr;

    const auto * lhs_column_node = expr_list_node->getNodes()[0]->as<ColumnNode>();
    const auto * rhs_column_node = expr_list_node->getNodes()[1]->as<ColumnNode>();
    if (!lhs_column_node || !rhs_column_node)
        return nullptr;

    /// If USING column resolved from projection, keep its name
    if (lhs_column_node->hasExpression())
        return nullptr;

    if (lhs_column_node->getColumnName() == rhs_column_node->getColumnName())
        return nullptr;

    auto node_ast = std::make_shared<ASTIdentifier>(lhs_column_node->getColumnName());
    node_ast->setAlias(rhs_column_node->getColumnName());
    return node_ast;
}

static ASTPtr makeUsingAST(const QueryTreeNodePtr & node)
{
    const auto & list_node = node->as<ListNode &>();

    auto expr_list = std::make_shared<ASTExpressionList>();
    expr_list->children.reserve(list_node.getNodes().size());

    for (const auto & child : list_node.getNodes())
    {
        ASTPtr node_ast = tryMakeUsingColumnASTWithAlias(child);

        if (!node_ast)
            node_ast = child->toAST();

        expr_list->children.push_back(std::move(node_ast));
    }

    return expr_list;
}

ASTPtr JoinNode::toASTTableJoin() const
{
    auto join_ast = std::make_shared<ASTTableJoin>();
    join_ast->locality = locality;
    join_ast->strictness = strictness;
    join_ast->kind = kind;

    if (children[join_expression_child_index])
    {
        if (is_using_join_expression)
        {
            join_ast->using_expression_list = makeUsingAST(children[join_expression_child_index]);
            join_ast->children.push_back(join_ast->using_expression_list);
        }
        else
        {
            join_ast->on_expression = children[join_expression_child_index]->toAST();
            join_ast->children.push_back(join_ast->on_expression);
        }
    }

    return join_ast;
}

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

    buffer << '\n' << std::string(indent + 2, ' ') << "RIGHT TABLE EXPRESSION\n";
    getRightTableExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    if (getJoinExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "JOIN EXPRESSION\n";
        getJoinExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool JoinNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const JoinNode &>(rhs);
    return locality == rhs_typed.locality && strictness == rhs_typed.strictness && kind == rhs_typed.kind &&
        is_using_join_expression == rhs_typed.is_using_join_expression;
}

void JoinNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    state.update(locality);
    state.update(strictness);
    state.update(kind);
    state.update(is_using_join_expression);
}

QueryTreeNodePtr JoinNode::cloneImpl() const
{
    return std::make_shared<JoinNode>(
        getLeftTableExpression(), getRightTableExpression(), getJoinExpression(),
        locality, strictness, kind, is_using_join_expression);
}

ASTPtr JoinNode::toASTImpl(const ConvertToASTOptions & options) const
{
    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();

    addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, children[left_table_expression_child_index], options);

    size_t join_table_index = tables_in_select_query_ast->children.size();

    auto join_ast = toASTTableJoin();

    addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, children[right_table_expression_child_index], options);

    auto & table_element = tables_in_select_query_ast->children.at(join_table_index)->as<ASTTablesInSelectQueryElement &>();
    table_element.children.push_back(std::move(join_ast));
    table_element.table_join = table_element.children.back();

    return tables_in_select_query_ast;
}

void JoinNode::crossToInner(const QueryTreeNodePtr & join_expression_)
{
    if (kind != JoinKind::Cross && kind != JoinKind::Comma)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot rewrite {} to INNER JOIN, expected CROSS", toString(kind));

    if (children[join_expression_child_index])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join expression is expected to be empty for CROSS JOIN, got '{}'",
            children[join_expression_child_index]->formatConvertedASTForErrorMessage());

    kind = JoinKind::Inner;
    strictness = JoinStrictness::All;
    children[join_expression_child_index] = join_expression_;
}


CrossJoinNode::CrossJoinNode(QueryTreeNodePtr table_expression)
    : IQueryTreeNode(1)
{
    children = {std::move(table_expression)};
}

CrossJoinNode::CrossJoinNode(QueryTreeNodes table_expressions, JoinTypes join_types_)
    : IQueryTreeNode(table_expressions.size())
    , join_types(std::move(join_types_))
{
    children = std::move(table_expressions);
    if (children.size() != join_types.size() + 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid number of join tables for CrossJoinNode. Expected {} got {}",
            join_types.size() + 1, children.size());
}

void CrossJoinNode::appendTable(QueryTreeNodePtr table_expression, CrossJoinNode::JoinType join_type)
{
    children.push_back(std::move(table_expression));
    join_types.push_back(join_type);
}

void CrossJoinNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CROSS JOIN id: " << format_state.getNodeId(this);

    for (const auto & child : children)
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "TABLE EXPRESSION\n";
        child->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool CrossJoinNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const CrossJoinNode &>(rhs);

    if (rhs_typed.join_types.size() != join_types.size())
        return false;

    for (size_t i = 0; i < join_types.size(); ++i)
        if (!(join_types[i].is_comma == rhs_typed.join_types[i].is_comma &&
              join_types[i].locality == rhs_typed.join_types[i].locality))
              return false;

    return true;
}

void CrossJoinNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    state.update(join_types.size());
    for (const auto & join_type : join_types)
    {
        state.update(join_type.is_comma);
        state.update(join_type.locality);
    }
}

QueryTreeNodePtr CrossJoinNode::cloneImpl() const
{
    return std::make_shared<CrossJoinNode>(children, join_types);
}

ASTPtr CrossJoinNode::toASTImpl(const ConvertToASTOptions & options) const
{
    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();

    for (size_t i = 0; i < children.size(); ++i)
    {
        const auto & child = children[i];
        size_t join_table_index = tables_in_select_query_ast->children.size();
        addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, child, options);

        if (i > 0)
        {
            auto join_ast = std::make_shared<ASTTableJoin>();
            join_ast->locality = join_types[i - 1].locality;
            join_ast->strictness = JoinStrictness::Unspecified;
            join_ast->kind = join_types[i - 1].is_comma ? JoinKind::Comma : JoinKind::Cross;

            auto & table_element = tables_in_select_query_ast->children.at(join_table_index)->as<ASTTablesInSelectQueryElement &>();
            table_element.children.push_back(std::move(join_ast));
            table_element.table_join = table_element.children.back();
        }
    }

    return tables_in_select_query_ast;
}

}
