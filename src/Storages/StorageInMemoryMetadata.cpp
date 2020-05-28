#include <Storages/StorageInMemoryMetadata.h>

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Poco/String.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_TTL_EXPRESSION;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
};

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


namespace
{

void checkTTLExpression(const ExpressionActionsPtr & ttl_expression, const String & result_column_name)
{
    for (const auto & action : ttl_expression->getActions())
    {
        if (action.type == ExpressionAction::APPLY_FUNCTION)
        {
            IFunctionBase & func = *action.function_base;
            if (!func.isDeterministic())
                throw Exception(
                    "TTL expression cannot contain non-deterministic functions, "
                    "but contains function "
                        + func.getName(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    const auto & result_column = ttl_expression->getSampleBlock().getByName(result_column_name);

    if (!typeid_cast<const DataTypeDateTime *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate *>(result_column.type.get()))
    {
        throw Exception(
            "TTL expression result column should have DateTime or Date type, but has " + result_column.type->getName(),
            ErrorCodes::BAD_TTL_EXPRESSION);
    }
}

}

StorageMetadataTTLField StorageMetadataTTLField::getTTLFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context)
{
    StorageMetadataTTLField result;
    const auto * ttl_element = definition_ast->as<ASTTTLElement>();

    /// First child is expression: `TTL expr TO DISK`
    if (ttl_element != nullptr)
        result.expression_ast = ttl_element->children.front()->clone();
    else /// It's columns TTL without any additions, just copy it
        result.expression_ast = definition_ast->clone();

    auto ttl_ast = result.expression_ast->clone();
    auto syntax_result = SyntaxAnalyzer(context).analyze(ttl_ast, columns.getAllPhysical());
    result.expression = ExpressionAnalyzer(ttl_ast, syntax_result, context).getActions(false);

    /// Move TTL to disk or volume
    if (ttl_element != nullptr)
    {
        result.destination_type = ttl_element->destination_type;
        result.destination_name = ttl_element->destination_name;
    }

    result.result_column = ttl_ast->getColumnName();

    checkTTLExpression(result.expression, result.result_column);
    return result;
}


StorageMetadataSkipIndexField StorageMetadataSkipIndexField::getSkipIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context)
{
    const auto * index_definition = definition_ast->as<ASTIndexDeclaration>();
    if (!index_definition)
        throw Exception("Cannot create skip index from non ASTIndexDeclaration AST", ErrorCodes::LOGICAL_ERROR);

    if (index_definition->name.empty())
        throw Exception("Skip index must have name in definition.", ErrorCodes::INCORRECT_QUERY);

    if (!index_definition->type)
        throw Exception("TYPE is required for index", ErrorCodes::INCORRECT_QUERY);

    if (index_definition->type->parameters && !index_definition->type->parameters->children.empty())
        throw Exception("Index type cannot have parameters", ErrorCodes::INCORRECT_QUERY);

    StorageMetadataSkipIndexField result;
    result.definition_ast = index_definition->clone();
    result.name = index_definition->name;
    result.type = Poco::toLower(index_definition->type->name);
    result.granularity = index_definition->granularity;

    ASTPtr expr_list = extractKeyExpressionList(index_definition->expr->clone());
    result.expression_list_ast = expr_list->clone();

    auto syntax = SyntaxAnalyzer(context).analyze(expr_list, columns.getAllPhysical());
    result.expression = ExpressionAnalyzer(expr_list, syntax, context).getActions(true);
    result.sample_block = result.expression->getSampleBlock();

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
    {
        const auto & column = result.sample_block.getByPosition(i);
        result.column_names.emplace_back(column.name);
        result.data_types.emplace_back(column.type);
    }

    const auto & definition_arguments = index_definition->type->arguments;
    if (definition_arguments)
    {
        for (size_t i = 0; i < definition_arguments->children.size(); ++i)
        {
            const auto * argument = definition_arguments->children[i]->as<ASTLiteral>();
            if (!argument)
                throw Exception("Only literals can be skip index arguments", ErrorCodes::INCORRECT_QUERY);
            result.arguments.emplace_back(argument->value);
        }
    }

    return result;
}


bool IndicesDescription::has(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return true;
    return false;
}

String IndicesDescription::toString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
        list.children.push_back(index.definition_ast);

    return serializeAST(list, true);
}


IndicesDescription IndicesDescription::parse(const String & str, const ColumnsDescription & columns, const Context & context)
{
    IndicesDescription result;
    if (str.empty())
        return result;

    ParserIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & index : list->children)
        result.emplace_back(StorageMetadataSkipIndexField::getSkipIndexFromAST(index, columns, context));

    return result;
}

}
