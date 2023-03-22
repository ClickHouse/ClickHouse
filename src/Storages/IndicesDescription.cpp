#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/IndicesDescription.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/extractKeyExpressionList.h>

#include <Core/Defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

IndexDescription::IndexDescription(const IndexDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , name(other.name)
    , type(other.type)
    , arguments(other.arguments)
    , column_names(other.column_names)
    , data_types(other.data_types)
    , sample_block(other.sample_block)
    , granularity(other.granularity)
{
    if (other.expression)
        expression = other.expression->clone();
}


IndexDescription & IndexDescription::operator=(const IndexDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    if (other.expression_list_ast)
        expression_list_ast = other.expression_list_ast->clone();
    else
        expression_list_ast.reset();

    name = other.name;
    type = other.type;

    if (other.expression)
        expression = other.expression->clone();
    else
        expression.reset();

    arguments = other.arguments;
    column_names = other.column_names;
    data_types = other.data_types;
    sample_block = other.sample_block;
    granularity = other.granularity;
    return *this;
}

IndexDescription IndexDescription::getIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context)
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

    IndexDescription result;
    result.definition_ast = index_definition->clone();
    result.name = index_definition->name;
    result.type = Poco::toLower(index_definition->type->name);
    result.granularity = index_definition->granularity;

    ASTPtr expr_list = extractKeyExpressionList(index_definition->expr->clone());
    result.expression_list_ast = expr_list->clone();

    auto syntax = TreeRewriter(context).analyze(expr_list, columns.getAllPhysical());
    result.expression = ExpressionAnalyzer(expr_list, syntax, context).getActions(true);
    Block block_without_columns = result.expression->getSampleBlock();

    for (size_t i = 0; i < block_without_columns.columns(); ++i)
    {
        const auto & column = block_without_columns.getByPosition(i);
        result.column_names.emplace_back(column.name);
        result.data_types.emplace_back(column.type);
        result.sample_block.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
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

void IndexDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context)
{
    *this = getIndexFromAST(definition_ast, new_columns, context);
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


IndicesDescription IndicesDescription::parse(const String & str, const ColumnsDescription & columns, ContextPtr context)
{
    IndicesDescription result;
    if (str.empty())
        return result;

    ParserIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & index : list->children)
        result.emplace_back(IndexDescription::getIndexFromAST(index, columns, context));

    return result;
}


ExpressionActionsPtr IndicesDescription::getSingleExpressionForIndices(const ColumnsDescription & columns, ContextPtr context) const
{
    ASTPtr combined_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & index : *this)
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

Names IndicesDescription::getAllRegisteredNames() const
{
    Names result;
    for (const auto & index : *this)
    {
        result.emplace_back(index.name);
    }
    return result;
}
}
