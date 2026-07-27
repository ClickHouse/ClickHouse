#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/IndicesDescription.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/extractKeyExpressionList.h>

#include <Storages/ReplaceAliasByExpressionVisitor.h>

#include <Core/Defines.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

IndexDescription::IndexDescription(const IndexDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , name(other.name)
    , type(other.type)
    , arguments(other.arguments ? other.arguments->clone() : nullptr)
    , column_names(other.column_names)
    , data_types(other.data_types)
    , sample_block(other.sample_block)
    , granularity(other.granularity)
    , is_implicitly_created(other.is_implicitly_created)
    , escape_filenames(other.escape_filenames)
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

    if (other.arguments)
        arguments = other.arguments->clone();
    else
        arguments.reset();

    column_names = other.column_names;
    data_types = other.data_types;
    sample_block = other.sample_block;
    granularity = other.granularity;
    is_implicitly_created = other.is_implicitly_created;
    escape_filenames = other.escape_filenames;
    return *this;
}

IndexDescription IndexDescription::getIndexFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    bool is_implicitly_created,
    bool escape_filenames,
    ContextPtr context)
{
    const auto * index_definition = definition_ast->as<ASTIndexDeclaration>();
    if (!index_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create skip index from non ASTIndexDeclaration AST");

    if (index_definition->name.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Skip index must have name in definition.");

    auto index_type = index_definition->getType();
    if (!index_type)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TYPE is required for index");

    if (index_type->parameters && !index_type->parameters->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index type cannot have parameters");

    IndexDescription result;
    result.definition_ast = index_definition->clone();
    result.name = index_definition->name;
    result.type = Poco::toLower(index_type->name);
    result.granularity = index_definition->granularity;
    result.is_implicitly_created = is_implicitly_created;
    result.escape_filenames = escape_filenames;

    checkExpressionDoesntContainSubqueries(*index_definition->getExpression());
    result.initExpressionInfo(index_definition->getExpression(), columns, context);

    for (auto & elem : result.sample_block)
    {
        if (!elem.column)
            elem.column = elem.type->createColumn();

        result.column_names.push_back(elem.name);
        result.data_types.push_back(elem.type);
    }

    if (result.column_names.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Skip index '{}' must have at least one column in its expression", result.name);

    if (index_type && index_type->arguments)
        result.arguments = index_type->arguments->clone();

    return result;
}

void IndexDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context)
{
    *this = getIndexFromAST(definition_ast, new_columns, is_implicitly_created, escape_filenames, context);
}

void IndexDescription::initExpressionInfo(ASTPtr index_expression, const ColumnsDescription & columns, ContextPtr context)
{
    chassert(index_expression != nullptr);

    using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasByExpressionMatcher, true>;

    ASTPtr expr_list = extractKeyExpressionList(index_expression);
    if (expr_list == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression is not set");

    ReplaceAliasToExprVisitor::Data data{columns};
    ReplaceAliasToExprVisitor{data}.visit(expr_list);

    expression_list_ast = expr_list->clone();

    TreeRewriterResultPtr syntax = TreeRewriter(context).analyze(
        expr_list,
        columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns())
    );

    expression = ExpressionAnalyzer(expr_list, syntax, context).getActions(true);

    sample_block = expression->getSampleBlock();
}

Field getFieldFromIndexArgumentAST(const ASTPtr & ast)
{
    /// E.g. INDEX index_name column_name TYPE vector_similarity('hnsw', 'f32')
    if (const auto * ast_literal = ast->as<ASTLiteral>())
        return ast_literal->value;
    /// E.g. INDEX index_name column_name TYPE vector_similarity(index_name, column_name)
    if (const auto * ast_identifier = ast->as<ASTIdentifier>())
        return Field(ast_identifier->name());
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Only literals and identifiers can be skip index arguments");
}

FieldVector getFieldsFromIndexArgumentsAST(const ASTPtr & arguments)
{
    FieldVector result;
    if (!arguments)
        return result;

    result.reserve(arguments->children.size());
    for (const auto & child : arguments->children)
        result.emplace_back(getFieldFromIndexArgumentAST(child));
    return result;
}

bool IndexDescription::isSimpleSingleColumnIndex() const
{
    return expression_list_ast && expression_list_ast->children.size() == 1 && expression_list_ast->children[0]->as<ASTIdentifier>();
}


bool IndicesDescription::has(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return true;
    return false;
}

const IndexDescription & IndicesDescription::getByName(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return index;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no index with name '{}'", name);
}

bool IndicesDescription::hasType(const String & type) const
{
    for (const auto & index : *this)
        if (index.type == type)
            return true;
    return false;
}

String IndicesDescription::explicitToString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
    {
        if (!index.isImplicitlyCreated())
            list.children.push_back(index.definition_ast);
    }

    return list.formatWithSecretsOneLine();
}

String IndicesDescription::allToString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
        list.children.push_back(index.definition_ast);

    return list.formatWithSecretsOneLine();
}


IndicesDescription
IndicesDescription::parse(const String & str, const ColumnsDescription & columns, bool escape_index_filenames, ContextPtr context)
{
    IndicesDescription result;
    if (str.empty())
        return result;

    ParserIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & index : list->children)
        result.emplace_back(
            IndexDescription::getIndexFromAST(index, columns, /* is_implicitly_created */ false, escape_index_filenames, context));

    return result;
}


ExpressionActionsPtr IndicesDescription::getSingleExpressionForIndices(const ColumnsDescription & columns, ContextPtr context) const
{
    ASTPtr combined_expr_list = make_intrusive<ASTExpressionList>();
    for (const auto & index : *this)
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
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

ASTPtr createImplicitMinMaxIndexAST(const String & column_name)
{
    auto index_type = makeASTFunction("minmax");
    auto index_ast = make_intrusive<ASTIndexDeclaration>(
        make_intrusive<ASTIdentifier>(column_name), index_type,
        IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX + column_name);

    index_ast->granularity = ASTIndexDeclaration::DEFAULT_INDEX_GRANULARITY;
    return index_ast;
}

IndexDescription createImplicitMinMaxIndexDescription(
    const String & column_name, const ColumnsDescription & columns, bool escape_index_filenames, ContextPtr context)
{
    auto index_ast = createImplicitMinMaxIndexAST(column_name);
    return IndexDescription::getIndexFromAST(index_ast, columns, /* is_implicitly_created */ true, escape_index_filenames, context);
}

}
