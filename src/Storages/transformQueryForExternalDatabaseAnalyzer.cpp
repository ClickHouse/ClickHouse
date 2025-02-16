#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Storages/transformQueryForExternalDatabaseAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Columns/ColumnConst.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ConstantValue.h>


#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
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
                node = std::make_shared<ConstantNode>(std::make_shared<ConstantValue>(out.str(), result_type));
            }
        }
    }
};

}

ASTPtr getASTForExternalDatabaseFromQueryTree(const QueryTreeNodePtr & query_tree)
{
    auto new_tree = query_tree->clone();

    PrepareForExternalDatabaseVisitor visitor;
    visitor.visit(new_tree);
    const auto * query_node = new_tree->as<QueryNode>();

    auto query_node_ast = query_node->toAST({ .add_cast_for_constants = false, .fully_qualified_identifiers = false });
    const IAST * ast = query_node_ast.get();

    if (const auto * ast_subquery = ast->as<ASTSubquery>())
        ast = ast_subquery->children.at(0).get();

    const auto * union_ast = ast->as<ASTSelectWithUnionQuery>();
    if (!union_ast)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST ({}) is not a ASTSelectWithUnionQuery", query_node_ast->getID());

    if (union_ast->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST is not a single ASTSelectQuery, got {}", union_ast->list_of_selects->children.size());

    return union_ast->list_of_selects->children.at(0);
}

}
