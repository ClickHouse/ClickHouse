#include <memory>
#include <optional>
#include <Analyzer/IQueryTreeNode.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/transformQueryForExternalDatabaseAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Columns/ColumnConst.h>

#include <Analyzer/Utils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

namespace
{

class PrepareForExternalDatabaseVisitor : public InDepthQueryTreeVisitor<PrepareForExternalDatabaseVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<PrepareForExternalDatabaseVisitor>;
    using Base::Base;

    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * constant_node = node->as<ConstantNode>();
        if (constant_node)
        {
            auto result_type = constant_node->getResultType();
            if (isDate(result_type) || isDateTime(result_type) || isDateTime64(result_type))
            {
                /// Use string representation of constant date and time values
                /// The code is ugly - how to convert artbitrary Field to proper string representation?
                /// (maybe we can just consider numbers as unix timestamps?)
                auto result_column = result_type->createColumnConst(1, constant_node->getValue());
                const IColumn & inner_column = result_column->getDataColumn();

                WriteBufferFromOwnString out;
                result_type->getDefaultSerialization()->serializeText(inner_column, 0, out, FormatSettings());
                node = std::make_shared<ConstantNode>(out.str(), std::move(result_type));
            }
        }
    }
};

/// A filter on the columns of one side of a join may run in that side's scan only when the join neither
/// extends that side with default or NULL rows nor picks one row per key from it: a pre-filter changes
/// which row is picked.
bool isFilterPushDownToJoinSideSafe(const JoinNode & join_node, JoinTableSide side)
{
    auto kind = join_node.getKind();
    if (isCrossOrComma(kind))
        return true;
    if (isFull(kind) || isPaste(kind))
        return false;
    if ((isLeft(kind) && side == JoinTableSide::Right) || (isRight(kind) && side == JoinTableSide::Left))
        return false;

    switch (join_node.getStrictness())
    {
        case JoinStrictness::All:
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
            return true;
        case JoinStrictness::Any:
            /// INNER ANY builds each key's row from the first row of either side that reaches it.
            return !isInner(kind);
        case JoinStrictness::RightAny:
            /// The legacy ANY pairs every left row with the first right row of its key.
            return side == JoinTableSide::Left;
        case JoinStrictness::Asof:
            /// The right side is searched for the closest row.
            return side == JoinTableSide::Left;
        case JoinStrictness::Unspecified:
            return false;
    }
}

/// `std::nullopt` when `table_expression` is not under `join_tree_node`.
std::optional<bool> isFilterPushDownToTableExpressionSafe(
    const QueryTreeNodePtr & join_tree_node, const QueryTreeNodePtr & table_expression)
{
    if (join_tree_node == table_expression)
        return true;

    switch (join_tree_node->getNodeType())
    {
        case QueryTreeNodeType::JOIN:
        {
            const auto & join_node = join_tree_node->as<const JoinNode &>();
            if (auto result = isFilterPushDownToTableExpressionSafe(join_node.getLeftTableExpressionNode(), table_expression))
                return *result && isFilterPushDownToJoinSideSafe(join_node, JoinTableSide::Left);
            if (auto result = isFilterPushDownToTableExpressionSafe(join_node.getRightTableExpressionNode(), table_expression))
                return *result && isFilterPushDownToJoinSideSafe(join_node, JoinTableSide::Right);
            return std::nullopt;
        }
        case QueryTreeNodeType::CROSS_JOIN:
        {
            for (const auto & table_expression_node : join_tree_node->as<const CrossJoinNode &>().getTableExpressions())
                if (auto result = isFilterPushDownToTableExpressionSafe(table_expression_node, table_expression))
                    return result;
            return std::nullopt;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            const auto & array_join_node = join_tree_node->as<const ArrayJoinNode &>();
            return isFilterPushDownToTableExpressionSafe(array_join_node.getTableExpressionNode(), table_expression);
        }
        default:
            return std::nullopt;
    }
}

}

ASTPtr getASTForExternalDatabaseFromQueryTree(ContextPtr context, const QueryTreeNodePtr & query_tree, const TableExpressionNodePtr & table_expression)
{
    auto replacement_table_expression = table_expression->clone();
    auto new_tree = query_tree->cloneAndReplace(table_expression, static_pointer_cast<ITableExpressionNode>(replacement_table_expression));

    PrepareForExternalDatabaseVisitor visitor;
    visitor.visit(new_tree);
    auto * query_node = new_tree->as<QueryNode>();

    const auto & join_tree = query_node->getJoinTreeNode();
    /// `cloneAndReplace` splices `replacement_table_expression` itself into the clone, so the walk finds it by pointer.
    bool allow_where = isFilterPushDownToTableExpressionSafe(join_tree, replacement_table_expression).value_or(false);

    /// Remove all sub-expressions (operands of AND) that depend on columns from other tables.
    /// This is needed for a correct push-down of these filters to an external storage.
    if (allow_where)
    {
        if (query_node->hasPrewhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getPrewhere(), replacement_table_expression, context);
        if (query_node->hasWhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getWhere(), replacement_table_expression, context);
    }

    /// The external database parses this text itself, so a date-time constant must stay in its text form.
    auto query_node_ast = query_node->toAST({ .add_cast_for_constants = false,
                                              .date_time_constants_as_numbers = false,
                                              .fully_qualified_identifiers = false });
    const IAST * ast = query_node_ast.get();

    if (const auto * ast_subquery = ast->as<ASTSubquery>())
        ast = ast_subquery->children.at(0).get();

    const auto * union_ast = ast->as<ASTSelectWithUnionQuery>();
    if (!union_ast)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST ({}) is not a ASTSelectWithUnionQuery", query_node_ast->getID());

    if (union_ast->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST is not a single ASTSelectQuery, got {}", union_ast->list_of_selects->children.size());

    ASTPtr select_query = union_ast->list_of_selects->children.at(0);
    auto * select_query_typed = select_query->as<ASTSelectQuery>();
    if (!select_query_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ASTSelectQuery, got {}", select_query ? select_query->formatForErrorMessage() : "nullptr");
    if (!allow_where)
    {
        /// Nothing is pushed down from this side of the join, so neither filter may reach the external
        /// database. `PREWHERE` has to go as well: the external table engines do not support it, so a
        /// surviving `PREWHERE` can only belong to the other, joined table and must not be presented to
        /// the caller as a filter on this one (`rejectOuterFilterForQueryBackedExternalSourceIfStrict`
        /// would otherwise reject it).
        select_query_typed->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
        select_query_typed->setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
    }
    return select_query;
}

}
