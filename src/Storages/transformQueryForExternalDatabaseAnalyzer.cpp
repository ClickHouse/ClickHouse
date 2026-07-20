#include <Parsers/ASTSubquery.h>
#include <Storages/transformQueryForExternalDatabaseAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Columns/ColumnConst.h>

#include <Analyzer/Utils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ConstantValue.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>

#include <DataTypes/DataTypesNumber.h>

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
                const IColumn & inner_column = assert_cast<const ColumnConst &>(*result_column).getDataColumn();

                WriteBufferFromOwnString out;
                result_type->getDefaultSerialization()->serializeText(inner_column, 0, out, FormatSettings());
                node = std::make_shared<ConstantNode>(out.str(), std::move(result_type));
            }
        }
    }
};

}

ASTPtr getASTForExternalDatabaseFromQueryTree(ContextPtr context, const QueryTreeNodePtr & query_tree, const QueryTreeNodePtr & table_expression)
{
    auto replacement_table_expression = table_expression->clone();
    auto new_tree = query_tree->cloneAndReplace(table_expression, replacement_table_expression);

    PrepareForExternalDatabaseVisitor visitor;
    visitor.visit(new_tree);
    auto * query_node = new_tree->as<QueryNode>();

    const auto & join_tree = query_node->getJoinTree();
    bool allow_where = true;
    if (const auto * join_node = join_tree->as<JoinNode>())
    {
        if (join_node->getKind() == JoinKind::Left)
            allow_where = join_node->getLeftTableExpression()->isEqual(*replacement_table_expression);
        else if (join_node->getKind() == JoinKind::Right)
            allow_where = join_node->getRightTableExpression()->isEqual(*replacement_table_expression);
        else
            allow_where = (join_node->getKind() == JoinKind::Inner);
    }

    /// Remove all sub-expressions (operands of AND) that depend on columns from other tables.
    /// This is needed for a correct push-down of these filters to an external storage.
    if (allow_where)
    {
        if (query_node->hasPrewhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getPrewhere(), replacement_table_expression, context);
        if (query_node->hasWhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getWhere(), replacement_table_expression, context);
    }

    auto query_node_ast = query_node->toAST({ .add_cast_for_constants = false, .fully_qualified_identifiers = false });
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
        select_query_typed->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
    return select_query;
}

}
