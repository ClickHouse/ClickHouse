#include <Storages/StorageInMemoryMetadata.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>

namespace DB
{

StorageInMemoryMetadata::StorageInMemoryMetadata(
    const ColumnsDescription & columns_,
    const IndicesDescription & indices_,
    const ConstraintsDescription & constraints_)
    : columns(columns_)
    , indices(indices_)
    , constraints(constraints_)
{
}

StorageInMemoryMetadata::StorageInMemoryMetadata(const StorageInMemoryMetadata & other)
    : columns(other.columns)
    , indices(other.indices)
    , constraints(other.constraints)
{
    if (other.partition_by_ast)
        partition_by_ast = other.partition_by_ast->clone();
    if (other.order_by_ast)
        order_by_ast = other.order_by_ast->clone();
    if (other.primary_key_ast)
        primary_key_ast = other.primary_key_ast->clone();
    if (other.ttl_for_table_ast)
        ttl_for_table_ast = other.ttl_for_table_ast->clone();
    if (other.sample_by_ast)
        sample_by_ast = other.sample_by_ast->clone();
    if (other.settings_ast)
        settings_ast = other.settings_ast->clone();
    if (other.select)
        select = other.select->clone();
}

StorageInMemoryMetadata & StorageInMemoryMetadata::operator=(const StorageInMemoryMetadata & other)
{
    if (this == &other)
        return *this;

    columns = other.columns;
    indices = other.indices;
    constraints = other.constraints;

    if (other.partition_by_ast)
        partition_by_ast = other.partition_by_ast->clone();
    else
        partition_by_ast.reset();

    if (other.order_by_ast)
        order_by_ast = other.order_by_ast->clone();
    else
        order_by_ast.reset();

    if (other.primary_key_ast)
        primary_key_ast = other.primary_key_ast->clone();
    else
        primary_key_ast.reset();

    if (other.ttl_for_table_ast)
        ttl_for_table_ast = other.ttl_for_table_ast->clone();
    else
        ttl_for_table_ast.reset();

    if (other.sample_by_ast)
        sample_by_ast = other.sample_by_ast->clone();
    else
        sample_by_ast.reset();

    if (other.settings_ast)
        settings_ast = other.settings_ast->clone();
    else
        settings_ast.reset();

    if (other.select)
        select = other.select->clone();
    else
        select.reset();

    return *this;
}

namespace
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node)
    {
        if (!node)
            return std::make_shared<ASTExpressionList>();

        const auto * expr_func = node->as<ASTFunction>();

        if (expr_func && expr_func->name == "tuple")
        {
            /// Primary key is specified in tuple, extract its arguments.
            return expr_func->arguments->clone();
        }
        else
        {
            /// Primary key consists of one column.
            auto res = std::make_shared<ASTExpressionList>();
            res->children.push_back(node);
            return res;
        }
    }
}

StorageMetadataKeyField StorageMetadataKeyField::getKeyFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context)
{
    StorageMetadataKeyField result;
    result.definition_ast = definition_ast;
    result.expression_list_ast = extractKeyExpressionList(definition_ast);

    if (result.expression_list_ast->children.empty())
        return result;

    const auto & children = result.expression_list_ast->children;
    for (const auto & child : children)
        result.column_names.emplace_back(child->getColumnName());

    {
        auto expr = result.expression_list_ast->clone();
        auto syntax_result = SyntaxAnalyzer(context).analyze(expr, columns.getAllPhysical());
        result.expression = ExpressionAnalyzer(expr, syntax_result, context).getActions(true);
        result.sample_block = result.expression->getSampleBlock();
    }

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
        result.data_types.emplace_back(result.sample_block.getByPosition(i).type);

    return result;
}

}
